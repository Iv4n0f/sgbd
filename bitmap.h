#pragma once
#include "disk.h"
#include <iostream>
#include <stdexcept>
#include <vector>

std::vector<char> readBlockLinear(int block_index, Disk &disk);
void writeBlockLinear(int block_index, const std::vector<char> &data,
                      Disk &disk);

struct BlockCoords {
  int plato;
  int superficie;
  int pista;
  int sector;
  int bloque;
};

class Bitmap {
  std::vector<bool> bits;
  int total_blocks;
  Disk &disk;
  int bitmap_block_index; // bloque donde se guarda el bitmap (lineal)

public:
  Bitmap(int total_blocks_, Disk &disk_, int bitmap_block_index_ = 1)
      : total_blocks(total_blocks_), disk(disk_),
        bitmap_block_index(bitmap_block_index_) {
    bits.resize(total_blocks, false); // todos libres
  }

  void set(int index, bool value) {
    if (index < 0 || index >= total_blocks)
      throw std::out_of_range("Bitmap set: índice fuera de rango");
    bits[index] = value;
  }

  bool get(int index) const {
    if (index < 0 || index >= total_blocks)
      throw std::out_of_range("Bitmap get: índice fuera de rango");
    return bits[index];
  }

  // Serializa el bitmap a un vector de chars, 1 bit por bloque, empaquetado en
  // bytes
  std::vector<char> serialize() const {
    std::vector<char> data(disk.block_size, 0); // tamaño bloque lleno
    for (int i = 0; i < total_blocks; ++i) {
      if (bits[i]) {
        data[i / 8] |= (1 << (i % 8));
      }
    }
    return data;
  }

  // Carga el bitmap desde un buffer serializado
  void deserialize(const std::vector<char> &data) {
    for (int i = 0; i < total_blocks; ++i) {
      bits[i] = (data[i / 8] & (1 << (i % 8))) != 0;
    }
  }

  // Convierte índice lineal a coordenadas físicas para Disk
  BlockCoords indexToCoords(int block_index) const;

  // Lee el bloque del bitmap desde el disco y actualiza bits
  void load() {
    auto data = readBlockLinear(bitmap_block_index, disk);
    deserialize(data);
  }

  // Guarda el bitmap en el disco en el bloque reservado
  void save() {
    auto data = serialize();
    writeBlockLinear(bitmap_block_index, data, disk);
  }
};
