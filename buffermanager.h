#pragma once

#include "disk.h"
#include <string>
#include <unordered_map>
#include <vector>

struct Frame {
  int block_id;
  bool dirty;
  int time;
  int pin_count;
  bool ref_bit;
  std::vector<char> data;
};

enum ReplacementPolicy { LRU, CLOCK };

class BufferManager {
public:
  BufferManager(Disk &disk_, int frame_count_, const std::string &policy);

  std::vector<char> &getBlock(int block_id);
  void markDirty(int block_id);
  void pin(int block_id);
  void unpin(int block_id);
  void flushBlock(int block_id);
  void flushAll();

  void printStatus() const;

private:
  Disk &disk;
  int frame_count;
  int current_time;
  int clock_hand;
  ReplacementPolicy replacement_policy;

  std::vector<Frame> frames;
  std::unordered_map<int, int> block_to_frame;

  void loadBlock(int block_id, int frame_index);
  int evictFrame();
  int evictLRU();
  int evictClock();

  void printStatusLRU() const;
  void printStatusClock() const;
};
