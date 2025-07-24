#include "hash_index.h"
#include "bitmap.h"
#include "disk.h"
#include <algorithm>
#include <cstring>

// Inicialización del mapa estático
std::map<std::string, HashIndex> HashIndex::indices;

// Constructor vacío
HashIndex::HashIndex()
    : header_block(-1), global_depth(1), key_size(0), bucket_capacity(0) {}

// Hash simple (FNV-1a)
uint32_t HashIndex::hashKey(const std::string &key) const {
  uint32_t hash = 2166136261u;
  for (char c : key) {
    hash ^= static_cast<uint8_t>(c);
    hash *= 16777619u;
  }
  return hash;
}

// Serialización de la cabecera
void HashIndex::serializeHeader(std::vector<char> &data) const {
  data.resize(12 + 4 * directory.size(), 0);
  std::memcpy(&data[0], &global_depth, 4);
  std::memcpy(&data[4], &key_size, 4);
  std::memcpy(&data[8], &bucket_capacity, 4);
  for (size_t i = 0; i < directory.size(); ++i) {
    std::memcpy(&data[12 + i * 4], &directory[i], 4);
  }
}

// Deserialización de la cabecera
void HashIndex::deserializeHeader(const std::vector<char> &data) {
  std::memcpy(&global_depth, &data[0], 4);
  std::memcpy(&key_size, &data[4], 4);
  std::memcpy(&bucket_capacity, &data[8], 4);
  size_t dir_size = 1 << global_depth;
  directory.resize(dir_size);
  for (size_t i = 0; i < dir_size; ++i) {
    std::memcpy(&directory[i], &data[12 + i * 4], 4);
  }
}

// Serialización de un bucket
void HashIndex::serializeBucket(const Bucket &bucket,
                                std::vector<char> &data) const {
  size_t entry_size = key_size + 4 + 4;
  data.resize(4 + 4 + entry_size * bucket.entries.size(), 0);
  std::memcpy(&data[0], &bucket.local_depth, 4);
  int n = bucket.entries.size();
  std::memcpy(&data[4], &n, 4);
  for (int i = 0; i < n; ++i) {
    const HashEntry &e = bucket.entries[i];
    std::memcpy(&data[8 + i * entry_size], e.key.data(), key_size);
    std::memcpy(&data[8 + i * entry_size + key_size], &e.block_idx, 4);
    std::memcpy(&data[8 + i * entry_size + key_size + 4], &e.offset, 4);
  }
}

// Deserialización de un bucket
void HashIndex::deserializeBucket(Bucket &bucket,
                                  const std::vector<char> &data) const {
  std::memcpy(&bucket.local_depth, &data[0], 4);
  int n = 0;
  std::memcpy(&n, &data[4], 4);
  bucket.entries.resize(n);
  size_t entry_size = key_size + 4 + 4;
  for (int i = 0; i < n; ++i) {
    bucket.entries[i].key.assign(&data[8 + i * entry_size], key_size);
    std::memcpy(&bucket.entries[i].block_idx,
                &data[8 + i * entry_size + key_size], 4);
    std::memcpy(&bucket.entries[i].offset,
                &data[8 + i * entry_size + key_size + 4], 4);
  }
}

// Crea un índice nuevo para una relación
void HashIndex::createForRelation(const std::string &relation_name, Disk &disk,
                                  Bitmap &bitmap, int key_size,
                                  int bucket_capacity) {
  // Reservar bloque de cabecera
  int header_block = bitmap.getFreeBlock();
  bitmap.set(header_block, true);

  // Inicializar directorio y buckets
  int global_depth = 1;
  std::vector<int> directory(2);
  std::map<int, Bucket> buckets;

  // Crear dos buckets iniciales
  for (int i = 0; i < 2; ++i) {
    int bucket_block = bitmap.getFreeBlock();
    bitmap.set(bucket_block, true);
    directory[i] = bucket_block;
    Bucket b;
    b.local_depth = 1;
    buckets[bucket_block] = b;
  }

  // Crear el índice y guardarlo en el mapa
  HashIndex idx;
  idx.header_block = header_block;
  idx.global_depth = global_depth;
  idx.key_size = key_size;
  idx.bucket_capacity = bucket_capacity;
  idx.directory = directory;
  idx.buckets = buckets;
  idx.saveToDisk(disk);
  indices[relation_name] = idx;
}

// Inserta una entrada en el índice
void HashIndex::insert(const std::string &key, int block_idx, int offset,
                       Disk &disk, Bitmap &bitmap) {
  uint32_t h = hashKey(key);
  int dir_idx = h & ((1 << global_depth) - 1);
  int bucket_block = directory[dir_idx];
  Bucket &bucket = buckets[bucket_block];

  // Verificar si la clave ya existe (no duplicar)
  for (const auto &e : bucket.entries) {
    if (e.key == key && e.block_idx == block_idx && e.offset == offset)
      return;
  }

  // Insertar
  if ((int)bucket.entries.size() < bucket_capacity) {
    bucket.entries.push_back({key, block_idx, offset});
    saveToDisk(disk);
    return;
  }

  // Si está lleno, dividir
  splitBucket(dir_idx, bitmap);
  // Reintentar la inserción
  insert(key, block_idx, offset, disk, bitmap);
}

// Divide un bucket lleno
void HashIndex::splitBucket(int dir_idx, Bitmap &bitmap) {
  int old_bucket_block = directory[dir_idx];
  Bucket &old_bucket = buckets[old_bucket_block];
  int old_local_depth = old_bucket.local_depth;

  // Si es necesario, duplicar el directorio
  if (old_local_depth == global_depth) {
    global_depth++;
    size_t old_size = directory.size();
    directory.resize(directory.size() * 2);
    for (size_t i = 0; i < old_size; ++i) {
      directory[i + old_size] = directory[i];
    }
  }

  // Crear nuevo bucket
  int new_bucket_block = bitmap.getFreeBlock();
  bitmap.set(new_bucket_block, true);
  Bucket new_bucket;
  new_bucket.local_depth = old_local_depth + 1;

  // Actualizar local_depth del bucket viejo
  old_bucket.local_depth++;

  // Redistribuir entradas
  std::vector<HashEntry> old_entries = old_bucket.entries;
  old_bucket.entries.clear();
  for (const auto &e : old_entries) {
    uint32_t h = hashKey(e.key);
    int idx = h & ((1 << global_depth) - 1);
    if ((idx & ((1 << old_bucket.local_depth) - 1)) ==
        (dir_idx & ((1 << old_bucket.local_depth) - 1))) {
      old_bucket.entries.push_back(e);
    } else {
      new_bucket.entries.push_back(e);
    }
  }

  // Actualizar directorio
  for (size_t i = 0; i < directory.size(); ++i) {
    if (directory[i] == old_bucket_block) {
      int mask = (1 << old_bucket.local_depth) - 1;
      if ((int(i) & mask) == (dir_idx & mask)) {
        directory[i] = old_bucket_block;
      } else {
        directory[i] = new_bucket_block;
      }
    }
  }

  buckets[new_bucket_block] = new_bucket;
}

// Busca todas las referencias para una clave
std::vector<std::pair<int, int>>
HashIndex::search(const std::string &key) const {
  uint32_t h = hashKey(key);
  int dir_idx = h & ((1 << global_depth) - 1);
  int bucket_block = directory[dir_idx];
  auto it = buckets.find(bucket_block);
  std::vector<std::pair<int, int>> result;
  if (it != buckets.end()) {
    for (const auto &e : it->second.entries) {
      if (e.key == key) {
        result.emplace_back(e.block_idx, e.offset);
      }
    }
  }
  return result;
}

// Elimina una entrada (si existe)
void HashIndex::remove(const std::string &key, int block_idx, int offset) {
  uint32_t h = hashKey(key);
  int dir_idx = h & ((1 << global_depth) - 1);
  int bucket_block = directory[dir_idx];
  Bucket &bucket = buckets[bucket_block];
  auto it = std::remove_if(
      bucket.entries.begin(), bucket.entries.end(), [&](const HashEntry &e) {
        return e.key == key && e.block_idx == block_idx && e.offset == offset;
      });
  if (it != bucket.entries.end()) {
    bucket.entries.erase(it, bucket.entries.end());
  }
}

// Serializa el índice completo a disco
void HashIndex::saveToDisk(Disk &disk) const {
  // Guardar cabecera
  std::vector<char> header_data;
  serializeHeader(header_data);
  header_data.resize(disk.block_size, 0);
  disk.writeBlock(header_block, header_data);

  // Guardar todos los buckets
  for (const auto &[block, bucket] : buckets) {
    std::vector<char> bucket_data;
    serializeBucket(bucket, bucket_data);
    bucket_data.resize(disk.block_size, 0);
    disk.writeBlock(block, bucket_data);
  }
}

// Carga el índice desde disco
void HashIndex::loadFromDisk(Disk &disk) {
  // Leer cabecera
  std::vector<char> header_data = disk.readBlock(header_block);
  deserializeHeader(header_data);

  // Leer todos los buckets
  buckets.clear();
  for (int block : directory) {
    if (buckets.count(block))
      continue; // Ya cargado
    std::vector<char> bucket_data = disk.readBlock(block);
    Bucket b;
    deserializeBucket(b, bucket_data);
    buckets[block] = b;
  }
}

// Carga todos los índices desde disco
void HashIndex::loadAllFromDisk(
    Disk &disk, const std::map<std::string, int> &relation_to_block) {
  indices.clear();
  for (const auto &[rel, block] : relation_to_block) {
    HashIndex idx;
    idx.header_block = block;
    idx.loadFromDisk(disk);
    indices[rel] = idx;
  }
}

// Guarda todos los índices a disco
void HashIndex::saveAllToDisk(Disk &disk) {
  for (auto &[rel, idx] : indices) {
    idx.saveToDisk(disk);
  }
}
