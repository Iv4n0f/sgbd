#include "catalog.h"
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

Catalog::Catalog(Disk &disk_) : disk(disk_) {}

bool Catalog::hasRelation(const std::string &name) const {
  return relations.find(name) != relations.end();
}

const Relation &Catalog::getRelation(const std::string &name) const {
  auto it = relations.find(name);
  if (it == relations.end()) {
    std::cerr << "ERROR: relacion '" << name << "' no encontrada" << std::endl;
    static Relation dummy;
    return dummy;
  }
  return it->second;
}

Relation &Catalog::getRelation(const std::string &name) {
  auto it = relations.find(name);
  if (it == relations.end()) {
    std::cerr << "ERROR: relacion '" << name << "' no encontrada" << std::endl;
    static Relation dummy;
    return dummy;
  }
  return it->second;
}

void Catalog::addRelation(const Relation &relation) {
  if (hasRelation(relation.name))
    throw std::runtime_error("La relación ya existe: " + relation.name);

  relations[relation.name] = relation;

  std::cout << "Nueva relación añadida...\n";
  size_t max_field_name_len = 0;
  for (auto field : relation.fields) {
    max_field_name_len = std::max(field.name.size(), max_field_name_len);
  }

  std::cout << "Nombre: " << relation.name << '\n';
  std::cout << "Tipo: " << (relation.is_fixed ? "Fijo" : "Variable") << '\n';
  std::cout << "Campos:\n";
  for (const auto &field : relation.fields) {
    std::cout << "  - " << std::left << std::setw(max_field_name_len)
              << field.name << " (" << field.type << ", " << field.size
              << ")\n";
  }
  std::cout << "Bloques asignados: ";
  for (size_t i = 0; i < relation.blocks.size(); ++i) {
    std::cout << relation.blocks[i];
    if (i + 1 < relation.blocks.size())
      std::cout << ", ";
  }
  if (relation.blocks.empty())
    std::cout << "ninguno";
  std::cout << "\n";
}

void Catalog::load() {
  std::vector<char> raw = disk.readBlock(1);
  std::istringstream iss(std::string(raw.data(), raw.size()));

  std::string line;
  while (std::getline(iss, line)) {
    if (line.empty())
      continue;

    std::istringstream header(line);
    Relation rel;
    std::string mode;
    int num_fields;

    header >> rel.name >> mode >> num_fields;

    if (rel.name.empty() || (mode != "fix" && mode != "var") || num_fields <= 0) {
      continue;
    }

    rel.is_fixed = (mode == "fix");

    for (int i = 0; i < num_fields; ++i) {
      if (!std::getline(iss, line)) break;
      std::istringstream field_line(line);
      Field f;
      field_line >> f.name >> f.type;
      if (rel.is_fixed)
        field_line >> f.size;
      else
        f.size = 0;
      rel.fields.push_back(f);
    }

    if (std::getline(iss, line)) {
      std::istringstream block_line(line);
      int block;
      while (block_line >> block) {
        rel.blocks.push_back(block);
      }
    }

    relations[rel.name] = rel;
  }
}

void Catalog::save() const {
  std::ostringstream oss;

  for (const auto &pair : relations) {
    const Relation &rel = pair.second;
    oss << rel.name << " " << (rel.is_fixed ? "fix" : "var") << " "
        << rel.fields.size() << "\n";
    for (const Field &f : rel.fields) {
      oss << f.name << " " << f.type;
      if (rel.is_fixed)
        oss << " " << f.size;
      oss << "\n";
    }
    for (int i = 0; i < (int)rel.blocks.size(); ++i) {
      oss << rel.blocks[i];
      if (i + 1 < (int)rel.blocks.size())
        oss << " ";
    }
    oss << "\n";
  }

  std::string content = oss.str();
  std::vector<char> block(disk.block_size, 0);
  std::memcpy(block.data(), content.data(),
              std::min(content.size(), block.size()));
  disk.writeBlock(1, block);
}

void Catalog::print() const {
  for (const auto &[name, rel] : relations) {
    size_t max_field_name_len = 0;
    for (auto field : rel.fields) {
      max_field_name_len = std::max(field.name.size(), max_field_name_len);
    }

    std::cout << "Nombre: " << rel.name << '\n';
    std::cout << "Tipo: " << (rel.is_fixed ? "Fijo" : "Variable") << '\n';
    std::cout << "Campos:\n";
    for (const auto &field : rel.fields) {
      std::cout << "  - " << std::left << std::setw(max_field_name_len)
                << field.name << " (" << field.type << ", " << field.size
                << ")\n";
    }
    std::cout << "Bloques asignados: ";
    for (size_t i = 0; i < rel.blocks.size(); ++i) {
      std::cout << rel.blocks[i];
      if (i + 1 < rel.blocks.size())
        std::cout << ", ";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
  std::cout << std::endl;
}

void Catalog::removeRelation(const std::string &name) {
  auto it = relations.find(name);
  if (it == relations.end())
    throw std::runtime_error("No se encontró la relación: " + name);
  relations.erase(it);
}

const std::unordered_map<std::string, Relation> &
Catalog::getAllRelations() const {
  return relations;
}
