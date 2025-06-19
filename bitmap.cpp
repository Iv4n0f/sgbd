#include "bitmap.h"
#include <cstring>
#include <stdexcept>

Bitmap::Bitmap(Disk &disk_) : disk(disk_) {
  int bloques_por_pista = disk.num_sectores / disk.sectors_per_block;
  int bloques_por_superficie = bloques_por_pista * disk.num_pistas;
  int bloques_por_plato = bloques_por_superficie * disk.num_superficies;
  total_blocks = bloques_por_plato * disk.num_platos;

  bits.resize(total_blocks, false);
}

void Bitmap::set(int index, bool value) {
  if (index < 0 || index >= total_blocks)
    throw std::out_of_range("Bitmap set: índice fuera de rango");
  bits[index] = value;
}

bool Bitmap::get(int index) const {
  if (index < 0 || index >= total_blocks)
    throw std::out_of_range("Bitmap get: índice fuera de rango");
  return bits[index];
}

bool Bitmap::load() {
  std::vector<char> data = disk.readBlock(0);
  if ((int)data.size() < (total_blocks + 7) / 8)
    return false;

  bits.assign(total_blocks, false);
  for (int i = 0; i < total_blocks; ++i) {
    bits[i] = (data[i / 8] & (1 << (i % 8))) != 0;
  }

  if (!bits[0] || !bits[1])
    return false;

  return true;
}

void Bitmap::save() const {
  std::vector<char> data((total_blocks + 7) / 8, 0);
  for (int i = 0; i < total_blocks; ++i) {
    if (bits[i]) {
      data[i / 8] |= (1 << (i % 8));
    }
  }

  std::vector<char> block(disk.block_size, 0);
  std::memcpy(block.data(), data.data(), std::min(data.size(), block.size()));

  disk.writeBlock(0, block);
}

int Bitmap::size() const { return total_blocks; }

int Bitmap::getFreeBlock() const {
  for (int i = 2; i < total_blocks; ++i) {
    if (!bits[i]) {
      return i;
    }
  }
  return -1;
}
