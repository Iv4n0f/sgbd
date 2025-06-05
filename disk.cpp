#include "disk.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

bool Disk::loadConfig(const std::string &filename, Disk::DiskConfig &cfg) {
  std::ifstream ifs(filename);
  if (!ifs)
    return false;

  std::string line;
  while (std::getline(ifs, line)) {
    std::istringstream iss(line);
    std::string key;
    if (std::getline(iss, key, '=')) {
      std::string value_str;
      if (std::getline(iss, value_str)) {
        int value = std::stoi(value_str);
        if (key == "platos")
          cfg.platos = value;
        else if (key == "pistas")
          cfg.pistas = value;
        else if (key == "sectores")
          cfg.sectores = value;
        else if (key == "block_size")
          cfg.block_size = value;
        else if (key == "blocks_per_sector")
          cfg.blocks_per_sector = value;
      }
    }
  }

  return true;
}

void Disk::saveConfig(const std::string &path, const DiskConfig &cfg) {
  std::ofstream ofs(path, std::ios::trunc);
  ofs << "platos=" << cfg.platos << "\n";
  ofs << "pistas=" << cfg.pistas << "\n";
  ofs << "sectores=" << cfg.sectores << "\n";
  ofs << "block_size=" << cfg.block_size << "\n";
  ofs << "blocks_per_sector=" << cfg.blocks_per_sector << "\n";
}

bool Disk::configChanged(const DiskConfig &a, const DiskConfig &b) {
  return a != b;
}

bool Disk::directoryIsComplete() {
  auto sample_sector =
      fs::path(root_path) / "plato0" / "superficie0" / "pista0" / "sector0";
  return fs::exists(sample_sector);
}

Disk::Disk(const std::string &root, const std::string &config)
    : root_path(root), config_file(config) {
  DiskConfig user_cfg{};
  if (!loadConfig(config_file, user_cfg)) {
    throw std::runtime_error("No se pudo cargar la configuración externa.");
  }

  DiskConfig internal_cfg{};
  std::string internal_config_path =
      (fs::path(root_path) / "disk.cfg").string();
  bool internal_exists = loadConfig(internal_config_path, internal_cfg);

  bool need_recreate = !internal_exists ||
                       configChanged(user_cfg, internal_cfg) ||
                       !directoryIsComplete();

  if (need_recreate) {
    if (fs::exists(root_path)) {
      fs::remove_all(root_path);
    }
    disk_config = user_cfg;

    num_platos = disk_config.platos;
    num_pistas = disk_config.pistas;
    num_sectores = disk_config.sectores;
    block_size = disk_config.block_size;
    blocks_per_sector = disk_config.blocks_per_sector;

    createStructure();
    saveConfig(internal_config_path, disk_config);
  } else {
    disk_config = internal_cfg;

    num_platos = disk_config.platos;
    num_pistas = disk_config.pistas;
    num_sectores = disk_config.sectores;
    block_size = disk_config.block_size;
    blocks_per_sector = disk_config.blocks_per_sector;
  }
}

void Disk::createStructure() {
  std::cout << "Creando estructura en " << root_path << std::endl;

  if (!fs::exists(root_path)) {
    if (!fs::create_directory(root_path)) {
      throw std::runtime_error("No se pudo crear el directorio raíz: " +
                               root_path);
    }
  }

  for (int plato = 0; plato < num_platos; ++plato) {
    std::string plato_dir = root_path + "/plato" + std::to_string(plato);
    std::cout << "Creando plato " << plato << std::endl;

    if (!fs::exists(plato_dir)) {
      if (!fs::create_directory(plato_dir)) {
        throw std::runtime_error("No se pudo crear el directorio plato: " +
                                 plato_dir);
      }
    }

    for (int superficie = 0; superficie < num_superficies; ++superficie) {
      std::string superficie_dir =
          plato_dir + "/superficie" + std::to_string(superficie);
      if (!fs::exists(superficie_dir)) {
        if (!fs::create_directory(superficie_dir)) {
          throw std::runtime_error(
              "No se pudo crear el directorio superficie: " + superficie_dir);
        }
      }

      for (int pista = 0; pista < num_pistas; ++pista) {
        std::string pista_dir =
            superficie_dir + "/pista" + std::to_string(pista);
        if (!fs::exists(pista_dir)) {
          if (!fs::create_directory(pista_dir)) {
            throw std::runtime_error("No se pudo crear el directorio pista: " +
                                     pista_dir);
          }
        }

        for (int sector = 0; sector < num_sectores; ++sector) {
          // Aquí sector es el archivo, que contiene varios bloques
          std::string sector_file =
              pista_dir + "/sector" + std::to_string(sector);

          if (!fs::exists(sector_file)) {
            std::ofstream ofs(sector_file, std::ios::binary);
            if (!ofs) {
              throw std::runtime_error("No se pudo crear archivo sector: " +
                                       sector_file);
            }

            std::vector<char> empty_sector(blocks_per_sector * block_size, 0);
            ofs.write(empty_sector.data(), empty_sector.size());
          }
        }
      }
    }
  }
}

std::vector<char> Disk::readBlock(int plato, int superficie, int pista,
                                  int sector, int bloque) {
  auto sector_file = fs::path(root_path) / ("plato" + std::to_string(plato)) /
                     ("superficie" + std::to_string(superficie)) /
                     ("pista" + std::to_string(pista)) /
                     ("sector" + std::to_string(sector));

  std::ifstream ifs(sector_file, std::ios::binary);
  if (!ifs) {
    throw std::runtime_error("Sector no encontrado");
  }

  std::vector<char> buffer(block_size);
  ifs.seekg(bloque * block_size);
  ifs.read(buffer.data(), block_size);
  return buffer;
}

void Disk::writeBlock(int plato, int superficie, int pista, int sector,
                      int bloque, const std::vector<char> &data) {
  if ((int)data.size() != block_size)
    throw std::runtime_error("Tamaño de datos incorrecto");

  auto sector_file = fs::path(root_path) / ("plato" + std::to_string(plato)) /
                     ("superficie" + std::to_string(superficie)) /
                     ("pista" + std::to_string(pista)) /
                     ("sector" + std::to_string(sector));

  std::fstream fs_sector(sector_file,
                         std::ios::in | std::ios::out | std::ios::binary);
  if (!fs_sector) {
    throw std::runtime_error("Sector no encontrado");
  }

  fs_sector.seekp(bloque * block_size);
  fs_sector.write(data.data(), block_size);
}

BlockPos Disk::blockPosFromIndex(int idx) {
  // Cantidad total de bloques verticales (una columna de todas las superficies de un plato)
  int bloques_por_columna = num_superficies * blocks_per_sector;

  // Total por sector: todas las columnas de todos los platos
  int bloques_por_sector = num_platos * bloques_por_columna;

  // Total por pista
  int bloques_por_pista = num_sectores * bloques_por_sector;

  // Primero obtenemos la pista
  int pista = idx / bloques_por_pista;
  idx %= bloques_por_pista;

  // Luego el sector
  int sector = idx / bloques_por_sector;
  idx %= bloques_por_sector;

  // Luego el plato
  int plato = idx / bloques_por_columna;
  idx %= bloques_por_columna;

  // Luego la superficie
  int superficie = idx / blocks_per_sector;

  // Finalmente el bloque dentro del sector
  int bloque = idx % blocks_per_sector;

  return {plato, superficie, pista, sector, bloque};
}

void Disk::printBlockPosition(int idx) {
  BlockPos pos = blockPosFromIndex(idx);
  std::cout << "Bloque " << idx << " ubicado en plato " << pos.plato
            << ", superficie " << pos.superficie << ", pista " << pos.pista
            << ", sector " << pos.sector << ", bloque " << pos.bloque
            << std::endl;
}

std::string Disk::getBlockPosition(int idx) {
  BlockPos pos = blockPosFromIndex(idx);
  return "plato " + std::to_string(pos.plato) + ", superficie " +
         std::to_string(pos.superficie) + ", pista " +
         std::to_string(pos.pista) + ", sector " + std::to_string(pos.sector) +
         ", bloque " + std::to_string(pos.bloque);
}

std::vector<char> Disk::readBlockByIndex(int idx) {
  BlockPos pos = blockPosFromIndex(idx);
  return readBlock(pos.plato, pos.superficie, pos.pista, pos.sector,
                   pos.bloque);
}

void Disk::writeBlockByIndex(int idx, const std::vector<char> &data) {
  BlockPos pos = blockPosFromIndex(idx);
  writeBlock(pos.plato, pos.superficie, pos.pista, pos.sector, pos.bloque,
             data);
}

void Disk::printDiskInfo() const {
  std::cout << "==== Información del Disco ====" << std::endl;
  std::cout << "Nro. de Platos: " << num_platos << std::endl;
  std::cout << "Nro. de Superficies por plato: " << num_superficies
            << std::endl;
  std::cout << "Nro. de Pistas por superficie: " << num_pistas << std::endl;
  std::cout << "Nro. de Sectores por pista: " << num_sectores << std::endl;
  std::cout << "Nro. Bloques por sector: " << blocks_per_sector << std::endl;

  int sector_size_bytes = block_size * blocks_per_sector;
  double sector_size_kb = static_cast<double>(sector_size_bytes) / 1024.0;
  double sector_size_mb = sector_size_kb / 1024.0;

  std::cout << "Capacidad del sector (KB): " << sector_size_kb << std::endl;
  std::cout << "Capacidad del sector (MB): " << sector_size_mb << std::endl;

  int bloques_por_pista = num_sectores * blocks_per_sector;
  std::cout << "Nro. de Bloques por pista: " << bloques_por_pista << std::endl;

  int bloques_por_superficie = bloques_por_pista * num_pistas;
  int bloques_por_plato = bloques_por_superficie * num_superficies;
  std::cout << "Nro. de Bloques por plato: " << bloques_por_plato << std::endl;

  int total_bloques = bloques_por_plato * num_platos;
  int capacidad_disco_bytes = total_bloques * block_size;
  double capacidad_disco_mb =
      static_cast<double>(capacidad_disco_bytes) / (1024.0 * 1024.0);
  std::cout << "Capacidad del disco (MB): " << capacidad_disco_mb << std::endl;

  std::cout << "Capacidad del bloque (bytes): " << block_size << std::endl;
  system("tree disk");
}
