#include "bitmap.h"

BlockCoords Bitmap::indexToCoords(int block_index) const {
  int blocks_per_sector = disk.blocks_per_sector;
  int sectors_per_pista = disk.num_sectores;
  int pistas_per_superficie = disk.num_pistas;
  int superficies = disk.num_superficies;
  int platos = disk.num_platos;

  int bloques_por_sector = blocks_per_sector;
  int bloques_por_pista = blocks_per_sector * sectors_per_pista;
  int bloques_por_superficie = bloques_por_pista * pistas_per_superficie;
  int bloques_por_plato = bloques_por_superficie * superficies;

  BlockCoords coords;
  coords.plato = block_index / bloques_por_plato;
  int rem_plato = block_index % bloques_por_plato;

  coords.superficie = rem_plato / bloques_por_superficie;
  int rem_superficie = rem_plato % bloques_por_superficie;

  coords.pista = rem_superficie / bloques_por_pista;
  int rem_pista = rem_superficie % bloques_por_pista;

  coords.sector = rem_pista / bloques_por_sector;
  coords.bloque = rem_pista % bloques_por_sector;

  return coords;
}

std::vector<char> readBlockLinear(int block_index, Disk& disk) {
  BlockCoords c = Bitmap(0, disk).indexToCoords(block_index);
  return disk.readBlock(c.plato, c.superficie, c.pista, c.sector, c.bloque);
}

void writeBlockLinear(int block_index, const std::vector<char>& data, Disk& disk) {
  BlockCoords c = Bitmap(0, disk).indexToCoords(block_index);
  disk.writeBlock(c.plato, c.superficie, c.pista, c.sector, c.bloque, data);
}
