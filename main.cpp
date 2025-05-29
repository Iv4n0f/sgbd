#include "bitmap.h"
#include "disk.h"
#include "metadata.h"
#include <iostream>

int main() {
  Disk disk("disk", "disk.cfg");

  DiskMetadata meta;
  MetadataManager mm(disk);

  if (!mm.load(meta)) {
    meta = {4, 16, 16, 4096, 4};
    mm.save(meta);
    std::cout << "Metadata creada y guardada.\n";
  } else {
    std::cout << "Metadata cargada.\n";
  }

  int total_blocks =
      meta.platos * meta.pistas * meta.sectores * meta.blocks_per_sector;

  Bitmap bitmap(total_blocks, disk, 1); // bitmap en bloque 1

  try {
    bitmap.load();
    std::cout << "Bitmap cargado desde disco.\n";
  } catch (...) {
    std::cout << "Bitmap no encontrado, inicializando...\n";
    bitmap.set(0, true);
    bitmap.set(1, true);
    bitmap.save();
    std::cout << "Bitmap inicializado y guardado.\n";
  }

  // Mostrar estado actual de algunos bloques
  std::cout << "Estado bloques tras carga/inicialización:\n";
  for (int i : {0, 1, 2}) {
    std::cout << "Bloque " << i << (bitmap.get(i) ? " ocupado\n" : " libre\n");
  }

  // Cambiar el estado de un bloque y guardar para probar persistencia
  std::cout << "Marcando bloque 2 como ocupado y guardando bitmap...\n";
  bitmap.save();

  // Simular recarga para probar persistencia
  Bitmap bitmap_reload(total_blocks, disk, 1);
  bitmap_reload.load();
  std::cout << "Bitmap recargado para verificar persistencia:\n";
  for (int i : {0, 1, 2}) {
    std::cout << "Bloque " << i
              << (bitmap_reload.get(i) ? " ocupado\n" : " libre\n");
  }

  auto data = disk.readBlock(0, 0, 0, 0, 1);
  for (int i = 0; i < 10; ++i) {
    for (int bit = 0; bit < 8; ++bit) {
      std::cout << ((data[i] & (1 << bit)) ? "1" : "0");
    }
    std::cout << " ";
  }
  std::cout << "\n";

  return 0;
}
