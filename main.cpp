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
    bitmap.set(0, true); 
    bitmap.set(1, true);
    bitmap.save();
  }

  std::cout << "Contenido del bitmap en disco (estado inicial):\n";
  auto data_inicial = disk.readBlock(0, 0, 0, 0, 0);
  printBitmapBits(data_inicial, 80);

  bitmap.set(10, false);
  std::cout << "Estado actual de bitmap en memoria ";
  std::cout << (bitmap.get(10)?"10 ocupado\n" : "10 libre\n");

  std::cout << "Contenido del bitmap en disco (antes de save):\n";
  data_inicial = disk.readBlock(0, 0, 0, 0, 0);
  printBitmapBits(data_inicial, 80);

  std::cout << "Contenido del bitmap en disco (despues de save):\n";
  bitmap.save();
  data_inicial = disk.readBlock(0, 0, 0, 0, 0);
  printBitmapBits(data_inicial, 80);

  std::cout<< "\n";
  return 0;
}
