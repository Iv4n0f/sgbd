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
  auto sample_sector = fs::path(root_path) / "plato0" / "superficie0" / "pista0" / "sector0";
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

  bool need_recreate = !internal_exists || configChanged(user_cfg, internal_cfg) || !directoryIsComplete();

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
      throw std::runtime_error("No se pudo crear el directorio raíz: " + root_path);
    }
  }

  for (int plato = 0; plato < num_platos; ++plato) {
    std::string plato_dir = root_path + "/plato" + std::to_string(plato);
    std::cout << "Creando plato " << plato << std::endl;

    if (!fs::exists(plato_dir)) {
      if (!fs::create_directory(plato_dir)) {
        throw std::runtime_error("No se pudo crear el directorio plato: " + plato_dir);
      }
    }

    for (int superficie = 0; superficie < num_superficies; ++superficie) {
      std::string superficie_dir = plato_dir + "/superficie" + std::to_string(superficie);
      if (!fs::exists(superficie_dir)) {
        if (!fs::create_directory(superficie_dir)) {
          throw std::runtime_error("No se pudo crear el directorio superficie: " + superficie_dir);
        }
      }

      for (int pista = 0; pista < num_pistas; ++pista) {
        std::string pista_dir = superficie_dir + "/pista" + std::to_string(pista);
        if (!fs::exists(pista_dir)) {
          if (!fs::create_directory(pista_dir)) {
            throw std::runtime_error("No se pudo crear el directorio pista: " + pista_dir);
          }
        }

        for (int sector = 0; sector < num_sectores; ++sector) {
          // Aquí sector es el archivo, que contiene varios bloques
          std::string sector_file = pista_dir + "/sector" + std::to_string(sector);

          if (!fs::exists(sector_file)) {
            std::ofstream ofs(sector_file, std::ios::binary);
            if (!ofs) {
              throw std::runtime_error("No se pudo crear archivo sector: " + sector_file);
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
  int bloques_por_plato = num_superficies * num_pistas * num_sectores * blocks_per_sector;

  int plato = idx / bloques_por_plato;
  idx = idx % bloques_por_plato;

  int bloques_por_superficie = num_pistas * num_sectores * blocks_per_sector;
  int superficie = idx / bloques_por_superficie;
  idx = idx % bloques_por_superficie;

  int bloques_por_pista = num_sectores * blocks_per_sector;
  int pista = idx / bloques_por_pista;
  idx = idx % bloques_por_pista;

  int bloques_por_sector = blocks_per_sector;
  int sector = idx / bloques_por_sector;
  int bloque = idx % bloques_por_sector;

  return {plato, superficie, pista, sector, bloque};
}

std::vector<char> Disk::readBlockByIndex(int idx) {
  BlockPos pos = blockPosFromIndex(idx);
  return readBlock(pos.plato, pos.superficie, pos.pista, pos.sector, pos.bloque);
}

void Disk::writeBlockByIndex(int idx, const std::vector<char> &data) {
  BlockPos pos = blockPosFromIndex(idx);
  writeBlock(pos.plato, pos.superficie, pos.pista, pos.sector, pos.bloque, data);
}
