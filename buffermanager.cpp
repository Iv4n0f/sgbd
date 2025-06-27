#include "buffermanager.h"
#include <iostream>
#include <iomanip>
#include <stdexcept>

BufferManager::BufferManager(Disk &disk_, int frame_count_, const std::string &policy)
  : disk(disk_), frame_count(frame_count_), current_time(0), clock_hand(0) {
  if (policy == "lru") replacement_policy = LRU;
  else if (policy == "clock") replacement_policy = CLOCK;
  else throw std::invalid_argument("Política de reemplazo no reconocida");

  frames.resize(frame_count);
  for (int i = 0; i < frame_count; ++i) {
    frames[i].block_id = -1;
    frames[i].dirty = false;
    frames[i].time = -1;
    frames[i].pin_count = 0;
    frames[i].ref_bit = false;
    frames[i].data.resize(disk.block_size);
  }
}

std::vector<char> &BufferManager::getBlock(int block_id) {
  ++current_time;

  auto it = block_to_frame.find(block_id);
  if (it != block_to_frame.end()) {
    int frame_idx = it->second;
    frames[frame_idx].time = current_time;
    if (replacement_policy == CLOCK) frames[frame_idx].ref_bit = true;
    return frames[frame_idx].data;
  }

  int frame_idx = evictFrame();

  if (frames[frame_idx].dirty && frames[frame_idx].block_id != -1) {
    disk.writeBlock(frames[frame_idx].block_id, frames[frame_idx].data);
  }

  loadBlock(block_id, frame_idx);
  block_to_frame[block_id] = frame_idx;
  return frames[frame_idx].data;
}

void BufferManager::markDirty(int block_id) {
  auto it = block_to_frame.find(block_id);
  if (it != block_to_frame.end()) {
    frames[it->second].dirty = true;
  }
}

void BufferManager::pin(int block_id) {
  auto it = block_to_frame.find(block_id);
  if (it != block_to_frame.end()) {
    frames[it->second].pin_count++;
  }
}

void BufferManager::unpin(int block_id) {
  auto it = block_to_frame.find(block_id);
  if (it != block_to_frame.end()) {
    if (frames[it->second].pin_count > 0) {
      frames[it->second].pin_count--;
    } else {
      throw std::runtime_error("Intento de unpin a un bloque no pineado");
    }
  }
}

void BufferManager::flushBlock(int block_id) {
  auto it = block_to_frame.find(block_id);
  if (it != block_to_frame.end()) {
    int idx = it->second;
    if (frames[idx].dirty) {
      disk.writeBlock(block_id, frames[idx].data);
      frames[idx].dirty = false;
    }
  }
}

void BufferManager::flushAll() {
  for (Frame &frame : frames) {
    if (frame.dirty && frame.block_id != -1) {
      disk.writeBlock(frame.block_id, frame.data);
      frame.dirty = false;
    }
  }
}

int BufferManager::evictFrame() {
  return (replacement_policy == LRU) ? evictLRU() : evictClock();
}

int BufferManager::evictLRU() {
  int oldest_time = current_time + 1;
  int evict_idx = -1;
  for (int i = 0; i < frame_count; ++i) {
    if (frames[i].block_id == -1)
      return i;
    if (frames[i].time < oldest_time && frames[i].pin_count == 0) {
      oldest_time = frames[i].time;
      evict_idx = i;
    }
  }
  if (evict_idx == -1)
    throw std::runtime_error("No se puede desalojar ningún frame (LRU)");

  block_to_frame.erase(frames[evict_idx].block_id);
  return evict_idx;
}

int BufferManager::evictClock() {
  int scanned = 0;
  while (scanned < frame_count * 2) {
    Frame &f = frames[clock_hand];
    if (f.pin_count == 0) {
      if (!f.ref_bit) {
        int evict_idx = clock_hand;
        if (f.block_id != -1)
          block_to_frame.erase(f.block_id);
        clock_hand = (clock_hand + 1) % frame_count;
        return evict_idx;
      } else {
        f.ref_bit = false;
      }
    }
    clock_hand = (clock_hand + 1) % frame_count;
    scanned++;
  }

  throw std::runtime_error("No se puede desalojar ningún frame (Clock)");
}

void BufferManager::loadBlock(int block_id, int frame_index) {
  frames[frame_index].data = disk.readBlock(block_id);
  frames[frame_index].block_id = block_id;
  frames[frame_index].dirty = false;
  frames[frame_index].time = current_time;
  frames[frame_index].pin_count = 0;
  frames[frame_index].ref_bit = (replacement_policy == CLOCK);
}

void BufferManager::printStatus() const {
  std::cout << "=== Estado del Buffer Manager (" 
            << (replacement_policy == LRU ? "LRU" : "Clock") 
            << ") ===\n";
  if (replacement_policy == LRU)
    printStatusLRU();
  else
    printStatusClock();
}

void BufferManager::printStatusLRU() const {
  const int w_idx = 8, w_block = 10, w_dirty = 8, w_time = 10, w_pincnt = 10;

  std::cout << std::left
            << std::setw(w_idx) << "Indice"
            << std::setw(w_block) << "Bloque"
            << std::setw(w_dirty) << "Dirty"
            << std::right
            << std::setw(w_time) << "Tiempo"
            << std::setw(w_pincnt) << "PinCount"
            << '\n';

  std::cout << std::string(w_idx + w_block + w_dirty + w_time + w_pincnt, '-') << "\n";

  for (int i = 0; i < frame_count; ++i) {
    const Frame &f = frames[i];
    std::cout << std::left
              << std::setw(w_idx) << i
              << std::setw(w_block) << f.block_id
              << std::setw(w_dirty) << (f.dirty ? "Si" : "No")
              << std::right
              << std::setw(w_time) << f.time
              << std::setw(w_pincnt) << f.pin_count
              << '\n';
  }
}

void BufferManager::printStatusClock() const {
  const int w_idx = 8, w_block = 10, w_dirty = 8, w_refbit = 10, w_pincnt = 10;

  std::cout << std::left
            << std::setw(w_idx) << "Indice"
            << std::setw(w_block) << "Bloque"
            << std::setw(w_dirty) << "Dirty"
            << std::setw(w_refbit) << "RefBit"
            << std::right
            << std::setw(w_pincnt) << "PinCount"
            << '\n';

  std::cout << std::string(w_idx + w_block + w_dirty + w_refbit + w_pincnt, '-') << "\n";

  for (int i = 0; i < frame_count; ++i) {
    const Frame &f = frames[i];
    std::cout << std::left
              << std::setw(w_idx) << i
              << std::setw(w_block) << f.block_id
              << std::setw(w_dirty) << (f.dirty ? "Si" : "No")
              << std::setw(w_refbit) << (f.ref_bit ? "1" : "0")
              << std::right
              << std::setw(w_pincnt) << f.pin_count;

    if (i == clock_hand)
      std::cout << "  <--- reloj";

    std::cout << '\n';
  }
}
