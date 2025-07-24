#pragma once
#include <vector>
#include <string>
#include <map>
#include <cstdint>

struct HashEntry {
    std::string key; // tamaño fijo
    int block_idx;
    int offset;
};

struct Bucket {
    int local_depth;
    std::vector<HashEntry> entries;
};

class Bitmap;
class Disk;

class HashIndex {
public:
    static std::map<std::string, HashIndex> indices;

    static void loadAllFromDisk(Disk& disk, const std::map<std::string, int>& relation_to_block);

    static void saveAllToDisk(Disk& disk);

    static void createForRelation(const std::string& relation_name, Disk& disk, Bitmap& bitmap, int key_size, int bucket_capacity);

    void insert(const std::string& key, int block_idx, int offset, Disk& disk, Bitmap& bitmap);
    void remove(const std::string& key, int block_idx, int offset);
    std::vector<std::pair<int, int>> search(const std::string& key) const;

    void loadFromDisk(Disk& disk);
    void saveToDisk(Disk& disk) const;

    int getHeaderBlock() const { return header_block; }

    HashIndex();

    int header_block; // bloque de cabecera en disco
    int global_depth;
    int key_size;
    int bucket_capacity;
    std::vector<int> directory; // directorio: hash -> bloque de bucket
    std::map<int, Bucket> buckets; // bloque -> bucket en memoria

    uint32_t hashKey(const std::string& key) const;
    void splitBucket(int dir_idx, Bitmap& bitmap);
    void serializeHeader(std::vector<char>& data) const;
    void deserializeHeader(const std::vector<char>& data);
    void serializeBucket(const Bucket& bucket, std::vector<char>& data) const;
    void deserializeBucket(Bucket& bucket, const std::vector<char>& data) const;
};