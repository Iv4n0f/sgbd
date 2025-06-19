#include "buffermanager.h"
#include <iomanip>
#include <iostream>
#include <stdexcept>

BufferManager::BufferManager(Disk &disk_, int frame_count_)
    : disk(disk_), frame_count(frame_count_), current_time(0) {
  frames.resize(frame_count);
  for (int i = 0; i < frame_count; ++i) {
    frames[i].block_id = -1;
    frames[i].dirty = false;
    frames[i].time = -1;
    frames[i].data.resize(disk.block_size);
    frames[i].pin_count = 0;
  }
}

std::vector<char> &BufferManager::getBlock(int block_id) {
  ++current_time;

  auto it = block_to_frame.find(block_id);
  if (it != block_to_frame.end()) {
    int frame_idx = it->second;
    frames[frame_idx].time = current_time;
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
    throw std::runtime_error("No se puede desalojar ningun frame");

  block_to_frame.erase(frames[evict_idx].block_id);
  return evict_idx;
}

void BufferManager::loadBlock(int block_id, int frame_index) {
  frames[frame_index].data = disk.readBlock(block_id);
  frames[frame_index].block_id = block_id;
  frames[frame_index].dirty = false;
  frames[frame_index].time = current_time;
  frames[frame_index].pin_count = 0;
}

void BufferManager::printStatus() const {
  std::cout << "=== Estado del Buffer Manager ===\n";

  const int w_idx    = 8;
  const int w_block  = 10;
  const int w_dirty  = 8;
  const int w_time   = 10;
  const int w_pincnt = 10;

  std::cout << std::left
            << std::setw(w_idx)    << "Índice"
            << std::setw(w_block)  << "Bloque"
            << std::setw(w_dirty)  << "Dirty"
            << std::right
            << std::setw(w_time)   << "Tiempo"
            << std::setw(w_pincnt) << "PinCount"
            << '\n';

  std::cout << std::string(w_idx + w_block + w_dirty + w_time + w_pincnt, '-') << "\n";

  for (int i = 0; i < frame_count; ++i) {
    const Frame &f = frames[i];
    std::cout << std::left
              << std::setw(w_idx)    << i
              << std::setw(w_block)  << f.block_id
              << std::setw(w_dirty)  << (f.dirty ? "Sí" : "No")
              << std::right
              << std::setw(w_time)   << f.time
              << std::setw(w_pincnt) << f.pin_count
              << '\n';
  }
}
