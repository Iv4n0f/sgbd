#pragma once

#include "disk.h"
#include <list>
#include <unordered_map>
#include <vector>

struct Frame {
  int block_id;
  std::vector<char> data;
  bool dirty;
  int time;
};

class BufferManager {
public:
  BufferManager(Disk &disk, int frame_count = 8);
  std::vector<char> &getBlock(int block_id);
  void markDirty(int block_id);
  void flushBlock(int block_id);
  void flushAll();
  void printStatus() const;

private:
  Disk &disk;
  int frame_count;
  int current_time;
  std::vector<Frame> frames;
  std::unordered_map<int, int> block_to_frame;
  int evictFrame();
  void loadBlock(int block_id, int frame_index);
};
