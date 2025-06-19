#pragma once
#include "disk.h"
#include <vector>

class Bitmap {
  std::vector<bool> bits;
  int total_blocks;
  Disk &disk;

public:
  Bitmap(Disk &disk_);
  void set(int index, bool value);
  bool get(int index) const;
  bool load();
  void save() const;
  int size() const;
  int getFreeBlock() const;
};
