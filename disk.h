#pragma once

#include <string>
#include <vector>

struct BlockPos {
  int plato;
  int superficie;
  int pista;
  int sector;
  int bloque;
};

class Disk {
public:
  std::string root_path;
  std::string config_file;

  int num_platos;
  int num_pistas;
  int num_sectores;
  int block_size;
  int blocks_per_sector;
  static constexpr int num_superficies = 2;

  struct DiskConfig {
    int platos;
    int pistas;
    int sectores;
    int block_size;
    int blocks_per_sector;

    bool operator==(const DiskConfig &other) const {
      return platos == other.platos && pistas == other.pistas &&
             sectores == other.sectores && block_size == other.block_size &&
             blocks_per_sector == other.blocks_per_sector;
    }

    bool operator!=(const DiskConfig &other) const { return !(*this == other); }
  };

  DiskConfig disk_config;

  bool loadConfig(const std::string &path, DiskConfig &cfg);
  void saveConfig(const std::string &path, const DiskConfig &cfg);
  bool configChanged(const DiskConfig &a, const DiskConfig &b);
  bool directoryIsComplete();
  Disk(const std::string &root, const std::string &config);
  void createStructure();

  std::vector<char> readBlock(int plato, int superficie, int pista, int sector,
                              int bloque);
  void writeBlock(int plato, int superficie, int pista, int sector, int bloque,
                  const std::vector<char> &data);

  BlockPos blockPosFromIndex(int idx);
  std::vector<char> readBlockByIndex(int idx);
  void writeBlockByIndex(int idx, const std::vector<char> &data);
};
