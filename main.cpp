#include "bitmap.h"
#include "disk.h"
#include <iostream>

void printBitmapBits(const std::vector<char> &data, int cantidad_bits) {
  for (int i = 0; i < (cantidad_bits + 7) / 8; ++i) {
    for (int bit = 0; bit < 8 && i * 8 + bit < cantidad_bits; ++bit) {
      std::cout << ((data[i] & (1 << bit)) ? "1" : "0");
    }
    std::cout << " ";
  }
  std::cout << "\n";
}

int main() {
  Disk disk("disk", "disk.cfg");
  Bitmap bitmap(disk);

  if (bitmap.load())
    std::cout << "Bitmap cargado desde disco.\n";
  else {
    std::cout << "Bitmap no encontrado. Inicializando...\n";
    bitmap.set(0, true); // bloque 0 reservado para el bitmap
    bitmap.set(1, true); // bloque 1 reservado para el catálogo
    bitmap.save();
  }

  std::cout << "Contenido del bitmap en disco (estado inicial):\n";
  auto data_inicial = disk.readBlock(0, 0, 0, 0, 0);
  printBitmapBits(data_inicial, 80);

  bitmap.set(10, false);
  bitmap.save();

  Bitmap bitmap_reloaded(disk);
  bitmap_reloaded.load();

  std::cout << "Contenido del bitmap en disco (estado final):\n";
  auto data_final = disk.readBlock(0, 0, 0, 0, 0);
  printBitmapBits(data_final, 80);

  return 0;
}
