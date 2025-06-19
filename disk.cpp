#include "disk.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

bool Disk::loadConfig(const std::string &filename, Disk::DiskConfig &cfg) {
  std::ifstream ifs(filename);
  if (!ifs) return false;

  std::string line;
  while (std::getline(ifs, line)) {
    std::istringstream iss(line);
    std::string key;
    if (std::getline(iss, key, '=')) {
      std::string value_str;
      if (std::getline(iss, value_str)) {
        int value = std::stoi(value_str);
        if (key == "platos") cfg.platos = value;
        else if (key == "pistas") cfg.pistas = value;
        else if (key == "sectores") cfg.sectores = value;
        else if (key == "sector_size") cfg.sector_size = value;
        else if (key == "sectors_per_block") cfg.sectors_per_block = value;
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
  ofs << "sector_size=" << cfg.sector_size << "\n";
  ofs << "sectors_per_block=" << cfg.sectors_per_block << "\n";
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
    throw std::runtime_error("No se pudo cargar la configuracion externa.");
  }

  DiskConfig internal_cfg{};
  std::string internal_config_path = (fs::path(root_path) / "disk.cfg").string();
  bool internal_exists = loadConfig(internal_config_path, internal_cfg);

  bool need_recreate = !internal_exists || configChanged(user_cfg, internal_cfg) || !directoryIsComplete();

  if (need_recreate) {
    if (fs::exists(root_path)) fs::remove_all(root_path);
    disk_config = user_cfg;

    num_platos = disk_config.platos;
    num_pistas = disk_config.pistas;
    num_sectores = disk_config.sectores;
    sector_size = disk_config.sector_size;
    sectors_per_block = disk_config.sectors_per_block;
    block_size = sector_size * sectors_per_block;

    createStructure();
    saveConfig(internal_config_path, disk_config);
  } else {
    disk_config = internal_cfg;

    num_platos = disk_config.platos;
    num_pistas = disk_config.pistas;
    num_sectores = disk_config.sectores;
    sector_size = disk_config.sector_size;
    sectors_per_block = disk_config.sectors_per_block;
    block_size = sector_size * sectors_per_block;
  }
}

void Disk::createStructure() {
  std::cout << "Creando estructura en " << root_path << std::endl;

  if (!fs::exists(root_path)) {
    if (!fs::create_directory(root_path)) {
      throw std::runtime_error("No se pudo crear el directorio raiz: " + root_path);
    }
  }

  for (int plato = 0; plato < num_platos; ++plato) {
    std::string plato_dir = root_path + "/plato" + std::to_string(plato);
    if (!fs::exists(plato_dir)) fs::create_directory(plato_dir);

    for (int superficie = 0; superficie < num_superficies; ++superficie) {
      std::string superficie_dir = plato_dir + "/superficie" + std::to_string(superficie);
      if (!fs::exists(superficie_dir)) fs::create_directory(superficie_dir);

      for (int pista = 0; pista < num_pistas; ++pista) {
        std::string pista_dir = superficie_dir + "/pista" + std::to_string(pista);
        if (!fs::exists(pista_dir)) fs::create_directory(pista_dir);

        for (int sector = 0; sector < num_sectores; ++sector) {
          std::string sector_file = pista_dir + "/sector" + std::to_string(sector);
          if (!fs::exists(sector_file)) {
            std::ofstream ofs(sector_file, std::ios::binary);
            std::vector<char> empty(sector_size, 0);
            ofs.write(empty.data(), sector_size);
          }
        }
      }
    }
  }
}

SectorPos Disk::sectorStartOfBlock(int block_idx) {
  int blocks_per_pista = num_sectores / sectors_per_block;
  int blocks_per_superficie = blocks_per_pista * num_pistas;
  int blocks_per_plato = blocks_per_superficie * num_superficies;

  int plato = block_idx / blocks_per_plato;
  block_idx %= blocks_per_plato;

  int superficie = block_idx / blocks_per_superficie;
  block_idx %= blocks_per_superficie;

  int pista = block_idx / blocks_per_pista;
  block_idx %= blocks_per_pista;

  int sector_inicial = block_idx * sectors_per_block;

  return {plato, superficie, pista, sector_inicial};
}

std::vector<char> Disk::readBlock(int block_idx) {
  SectorPos pos = sectorStartOfBlock(block_idx);
  std::vector<char> data(block_size);

  for (int i = 0; i < sectors_per_block; ++i) {
    std::string sector_file = fs::path(root_path) /
                              ("plato" + std::to_string(pos.plato)) /
                              ("superficie" + std::to_string(pos.superficie)) /
                              ("pista" + std::to_string(pos.pista)) /
                              ("sector" + std::to_string(pos.sector + i));

    std::ifstream ifs(sector_file, std::ios::binary);
    if (!ifs) throw std::runtime_error("Sector no encontrado: " + sector_file);

    ifs.read(&data[i * sector_size], sector_size);
  }

  return data;
}

void Disk::writeBlock(int block_idx, const std::vector<char> &data) {
  if ((int)data.size() != block_size)
    throw std::runtime_error("Tamaño de bloque incorrecto");

  SectorPos pos = sectorStartOfBlock(block_idx);

  for (int i = 0; i < sectors_per_block; ++i) {
    std::string sector_file = fs::path(root_path) /
                              ("plato" + std::to_string(pos.plato)) /
                              ("superficie" + std::to_string(pos.superficie)) /
                              ("pista" + std::to_string(pos.pista)) /
                              ("sector" + std::to_string(pos.sector + i));

    std::ofstream ofs(sector_file, std::ios::binary);
    if (!ofs) throw std::runtime_error("Sector no encontrado: " + sector_file);

    ofs.write(&data[i * sector_size], sector_size);
  }
}

void Disk::printBlockPosition(int block_idx) {
  SectorPos start = sectorStartOfBlock(block_idx);
  std::cout << "Bloque " << block_idx << " ubicado en:\n";
  std::cout << "  Plato: " << start.plato
            << ", Superficie: " << start.superficie
            << ", Pista: " << start.pista
            << ", Sectores: ";
  for (int i = 0; i < sectors_per_block; ++i) {
    std::cout << (start.sector + i);
    if (i < sectors_per_block - 1)
      std::cout << ", ";
  }
  std::cout << std::endl;
}

std::string Disk::getBlockPosition(int block_idx) {
  SectorPos start = sectorStartOfBlock(block_idx);
  std::string res = "plato " + std::to_string(start.plato) +
                    ", superficie " + std::to_string(start.superficie) +
                    ", pista " + std::to_string(start.pista) +
                    ", sectores: ";
  for (int i = 0; i < sectors_per_block; ++i) {
    res += std::to_string(start.sector + i);
    if (i < sectors_per_block - 1)
      res += ", ";
  }
  return res;
}

void Disk::printDiskInfo() const {
  std::cout << "==== Informacion del Disco ====" << std::endl;
  std::cout << "Platos: " << num_platos << std::endl;
  std::cout << "Superficies por plato: " << num_superficies << std::endl;
  std::cout << "Pistas por superficie: " << num_pistas << std::endl;
  std::cout << "Sectores por pista: " << num_sectores << std::endl;
  std::cout << "Sector size: " << sector_size << " bytes" << std::endl;
  std::cout << "Sectores por bloque: " << sectors_per_block << std::endl;

  int total_blocks = (num_platos * num_superficies * num_pistas * num_sectores) / sectors_per_block;

  std::cout << "Total de bloques: " << total_blocks << std::endl;
  std::cout << "Tamaño de bloque: " << block_size << " bytes" << std::endl;
  std::cout << "Capacidad total (MB): "
            << static_cast<double>(total_blocks * block_size) / (1024.0 * 1024.0)
            << std::endl;

  system("tree disk");
}
