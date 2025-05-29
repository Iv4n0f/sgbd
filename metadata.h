// metadata.h
#pragma once
#include <string>
#include <sstream>
#include <vector>
#include <iostream>
#include <cstring> // memcpy


struct DiskMetadata {
  int platos;
  int pistas;
  int sectores;
  int block_size;
  int blocks_per_sector;

  std::string serialize() const {
    std::ostringstream oss;
    oss << "platos=" << platos << "\n";
    oss << "pistas=" << pistas << "\n";
    oss << "sectores=" << sectores << "\n";
    oss << "block_size=" << block_size << "\n";
    oss << "blocks_per_sector=" << blocks_per_sector << "\n";
    return oss.str();
  }

  void deserialize(const std::string& s) {
    std::istringstream iss(s);
    std::string line;
    while (std::getline(iss, line)) {
      auto pos = line.find('=');
      if (pos == std::string::npos) continue;
      std::string key = line.substr(0, pos);
      std::string val = line.substr(pos + 1);

      if (key == "platos") platos = std::stoi(val);
      else if (key == "pistas") pistas = std::stoi(val);
      else if (key == "sectores") sectores = std::stoi(val);
      else if (key == "block_size") block_size = std::stoi(val);
      else if (key == "blocks_per_sector") blocks_per_sector = std::stoi(val);
    }
  }
};

class MetadataManager {
public:
  MetadataManager(Disk& disk) : disk_(disk) {}

  bool load(DiskMetadata& meta) {
    std::vector<char> block = disk_.readBlock(0, 0, 0, 0, 0);
    std::string data(block.begin(), block.end());
    if (data.find("platos=") == std::string::npos) {
      // No metadata found
      return false;
    }
    meta.deserialize(data);
    return true;
  }

  void save(const DiskMetadata& meta) {
    std::string data = meta.serialize();
    std::vector<char> block(disk_.block_size, 0);
    memcpy(block.data(), data.c_str(), std::min(data.size(), block.size()));
    disk_.writeBlock(0, 0, 0, 0, 0, block);
  }

private:
  Disk& disk_;
};
