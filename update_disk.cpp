#include "disk.h"
#include <iostream>

int main_fake(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Uso: " << argv[0] << " <ruta_disco> <config_file>\n";
    return 1;
  }

  std::string disk_root = argv[1];
  std::string config_file = argv[2];

  try {
    Disk disk(disk_root, config_file);

    std::cout << "Disco inicializado con la siguiente configuración:\n";
    std::cout << "Platos: " << disk.num_platos << "\n";
    std::cout << "Superficies: " << disk.num_superficies << "\n";
    std::cout << "Pistas: " << disk.num_pistas << "\n";
    std::cout << "Sectores: " << disk.num_sectores << "\n";
    std::cout << "Blocks per sector: " << disk.blocks_per_sector << "\n";
    std::cout << "Block size: " << disk.block_size << " bytes\n";

  } catch (const std::exception& e) {
    std::cerr << "Error al inicializar disco: " << e.what() << "\n";
    return 1;
  }

  return 0;
}
