#pragma once

#include <string>
#include <vector>

struct SectorPos {
  int plato;
  int superficie;
  int pista;
  int sector;
};

class Disk {
public:
  std::string root_path;
  std::string config_file;

  int num_platos;
  int num_pistas;
  int num_sectores;
  int sector_size;
  int sectors_per_block;
  int block_size;
  static constexpr int num_superficies = 2;

  struct DiskConfig {
    int platos;
    int pistas;
    int sectores;
    int sector_size;
    int sectors_per_block;

    bool operator==(const DiskConfig &other) const {
      return platos == other.platos && pistas == other.pistas &&
             sectores == other.sectores && sector_size == other.sector_size &&
             sectors_per_block == other.sectors_per_block;
    }

    bool operator!=(const DiskConfig &other) const {
      return !(*this == other);
    }
  };

  DiskConfig disk_config;

  // Configuracion y estructura
  bool loadConfig(const std::string &path, DiskConfig &cfg);
  void saveConfig(const std::string &path, const DiskConfig &cfg);
  bool configChanged(const DiskConfig &a, const DiskConfig &b);
  bool directoryIsComplete();
  Disk(const std::string &root, const std::string &config);
  void createStructure();

  // Acceso a bloques logicos
  std::vector<char> readBlock(int block_idx);
  void writeBlock(int block_idx, const std::vector<char> &data);

  // Utilidades
  SectorPos sectorStartOfBlock(int block_idx);
  void printBlockPosition(int block_idx);
  std::string getBlockPosition(int block_idx);
  void printDiskInfo() const;
};
