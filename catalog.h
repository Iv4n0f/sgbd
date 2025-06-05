#pragma once

#include "disk.h"
#include <string>
#include <unordered_map>
#include <vector>

struct Field {
  std::string name;
  std::string type;
  int size;
};

struct Relation {
  std::string name;
  bool is_fixed;
  std::vector<Field> fields;
  std::vector<int> blocks;
};

class Catalog {
public:
  Catalog(Disk &disk);

  void load();
  void save() const;
  void addRelation(const Relation &relation);
  bool hasRelation(const std::string &name) const;
  const Relation &getRelation(const std::string &name) const;
  Relation &getRelation(const std::string &name);
  void removeRelation(const std::string &name);
  void print() const;
  const std::unordered_map<std::string, Relation> &getAllRelations() const;

private:
  Disk &disk;
  std::unordered_map<std::string, Relation> relations;
};
