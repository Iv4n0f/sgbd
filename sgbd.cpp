#include "sgbd.h"
#include <algorithm>
#include <cctype>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <unordered_set>

static std::string trim(const std::string &s) {
  size_t start = 0;
  while (start < s.size() &&
         std::isspace(static_cast<unsigned char>(s[start]))) {
    ++start;
  }
  size_t end = s.size();
  while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
    --end;
  }
  return s.substr(start, end - start);
}

SGBD::SGBD(Disk &disk_) : disk(disk_), bitmap(disk_), catalog(disk_) {
  if (bitmap.load()) {
  } else {
    std::cout << "Bitmap no encontrado. Inicializando..." << std::endl;
    bitmap.set(0, true);
    bitmap.set(1, true);
    bitmap.save();
  }

  catalog.load();
}

std::vector<std::string> parseCSVLine(const std::string &line) {
  std::vector<std::string> result;
  std::string cur;
  bool in_quotes = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char c = line[i];
    if (c == '"') {
      in_quotes = !in_quotes;
    } else if (c == ',' && !in_quotes) {
      result.push_back(trim(cur));
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  result.push_back(trim(cur));
  return result;
}

int calculateRecordSize(const std::vector<Field> &fields) {
  int size = 0;
  for (const Field &f : fields) {
    size += f.size;
  }
  return size;
}

void SGBD::createOrReplaceRelation(const std::string &name, bool is_fixed,
                                   const std::vector<Field> &fields) {
  if (catalog.hasRelation(name)) {
    deleteRelation(name);
  }

  Relation rel;
  rel.name = name;
  rel.is_fixed = is_fixed;
  rel.fields = fields;

  int block = bitmap.getFreeBlock();

  if (block == -1) {
    throw std::runtime_error("No hay bloques libres para la nueva relación");
  }

  bitmap.set(block, true);

  if (is_fixed) {
    int record_size = calculateRecordSize(fields);
    initializeBlockHeader_fix(block, record_size);
  } else {
    // TODO: inicializar header para tamaño variable
  }

  rel.blocks.push_back(block);
  catalog.addRelation(rel);

  bitmap.save();
  catalog.save();
}

void SGBD::printStatus() const {
  std::cout << std::endl;
  std::cout << "Estado del Bitmap (primeros 80 bits):" << std::endl;
  for (int i = 0; i < 80; ++i) {
    std::cout << (bitmap.get(i) ? '1' : '0');
    if ((i + 1) % 8 == 0)
      std::cout << " ";
  }
  std::cout << "\n\nCatálogo:" << std::endl;
  std::cout << std::endl;
  catalog.print();
}

// suficiente para offsets de 4/8 kb
static std::string intTo4CharStr(int n) {
  char buf[5];
  if (n >= 0 && n <= 9999) {
    snprintf(buf, sizeof(buf), "%04d", n);
  } else if (n < 0 && n >= -999) {
    snprintf(buf, sizeof(buf), "%04d", n);
  } else {
    return "####";
  }
  return std::string(buf);
}

void SGBD::initializeBlockHeader_fix(int block_idx, int record_size) {
  int capacity = (disk.block_size - HEADER_SIZE_FIX) / record_size;
  int free_list_head = -1;
  int active_records = 0;

  std::vector<char> block_data(disk.block_size, 0);

  std::string header_str =
      intTo4CharStr(free_list_head) + intTo4CharStr(record_size) +
      intTo4CharStr(capacity) + intTo4CharStr(active_records);

  std::copy(header_str.begin(), header_str.end(), block_data.begin());

  disk.writeBlockByIndex(block_idx, block_data);
}

bool SGBD::insertRecord_fix(int block_idx, const std::vector<char> &record) {
  std::vector<char> block = disk.readBlockByIndex(block_idx);

  int free_list_head = std::stoi(std::string(block.begin(), block.begin() + 4));
  int record_size =
      std::stoi(std::string(block.begin() + 4, block.begin() + 8));
  int capacity = std::stoi(std::string(block.begin() + 8, block.begin() + 12));
  int active_records =
      std::stoi(std::string(block.begin() + 12, block.begin() + 16));

  if (record.size() != (size_t)record_size) {
    std::cerr << "Error: tamaño del registro no coincide" << std::endl;
    return false;
  }

  if (active_records >= capacity && free_list_head == -1) {
    return false;
  }

  int insert_pos;

  if (free_list_head == -1) {
    insert_pos = active_records;
  } else {
    insert_pos = free_list_head;

    int reg_offset = HEADER_SIZE_FIX + insert_pos * record_size;
    std::string next_str(block.begin() + reg_offset,
                         block.begin() + reg_offset + 4);
    int next_free = std::stoi(next_str);

    std::string next_free_str = intTo4CharStr(next_free);
    std::copy(next_free_str.begin(), next_free_str.end(), block.begin());
  }

  active_records++;
  std::string new_active = intTo4CharStr(active_records);
  std::copy(new_active.begin(), new_active.end(), block.begin() + 12);

  int final_offset = HEADER_SIZE_FIX + insert_pos * record_size;
  std::fill(block.begin() + final_offset,
            block.begin() + final_offset + record_size, 0);
  std::copy(record.begin(), record.end(), block.begin() + final_offset);

  disk.writeBlockByIndex(block_idx, block);

  return true;
}

bool SGBD::insert_fix(Relation &rel, const std::vector<char> &record) {
  if (!rel.is_fixed) {
    std::cerr << "La relación '" << rel.name << "' no es de registros fijos"
              << std::endl;
    return false;
  }

  int record_size = calculateRecordSize(rel.fields);
  if ((int)record.size() > record_size) {
    std::cerr << "Registro demasiado grande para la relación" << std::endl;
    return false;
  }

  for (int block_idx : rel.blocks) {
    if (insertRecord_fix(block_idx, record)) {
      return true;
    }
  }

  int new_block = bitmap.getFreeBlock();
  if (new_block == -1) {
    std::cerr << "No hay bloques libres disponibles para insertar" << std::endl;
    return false;
  }
  bitmap.set(new_block, true);
  initializeBlockHeader_fix(new_block, record_size);

  if (!insertRecord_fix(new_block, record)) {
    std::cerr << "Error insertando en bloque nuevo ERROR CRITICO" << std::endl;
    return false;
  }

  rel.blocks.push_back(new_block);
  bitmap.save();
  catalog.save();

  return true;
}

void SGBD::printRelation_fix(const std::string &relation_name) {
  std::cout << std::endl;
  if (!catalog.hasRelation(relation_name)) {
    std::cout << "Relación no encontrada: " << relation_name << std::endl;
    return;
  }

  const Relation &rel = catalog.getRelation(relation_name);
  if (!rel.is_fixed) {
    std::cout << "La relación no es de tamaño fijo." << std::endl;
    return;
  }

  int record_size = 0;
  std::vector<int> column_widths;
  for (const auto &f : rel.fields) {
    record_size += f.size;
    column_widths.push_back(std::max((int)f.name.size(), f.size));
  }

  int total_width = 2;
  for (int w : column_widths)
    total_width += w + 1;

  std::string separator(total_width, '-');
  std::cout << separator << std::endl;

  std::cout << "|";
  for (size_t i = 0; i < rel.fields.size(); ++i) {
    std::cout << " " << std::left << std::setw(column_widths[i])
              << rel.fields[i].name;
  }
  std::cout << " |" << std::endl;
  std::cout << separator << std::endl;

  for (int block_idx : rel.blocks) {
    std::vector<char> block = disk.readBlockByIndex(block_idx);

    int free_list_head_header =
        std::stoi(std::string(block.begin(), block.begin() + 4));
    int record_size_header =
        std::stoi(std::string(block.begin() + 4, block.begin() + 8));
    int active_records_header =
        std::stoi(std::string(block.begin() + 12, block.begin() + 16));

    if (record_size_header != record_size) {
      std::cout << "Error: tamaño de registro inconsistente en bloque "
                << block_idx << std::endl;
      continue;
    }

    std::unordered_set<int> deleted_records;
    int current = free_list_head_header;
    while (current != -1) {
      deleted_records.insert(current);
      int reg_offset = HEADER_SIZE_FIX + current * record_size;
      std::string next_str(block.begin() + reg_offset,
                           block.begin() + reg_offset + 4);
      int next_free = std::stoi(next_str);
      current = next_free;
    }

    int total_records = active_records_header + (int)deleted_records.size();
    int offset = HEADER_SIZE_FIX;

    for (int i = 0; i < total_records; ++i) {
      if (deleted_records.count(i) == 0) {
        std::cout << "|";
        int field_offset = 0;
        for (size_t j = 0; j < rel.fields.size(); ++j) {
          const auto &f = rel.fields[j];
          std::string field_data(block.data() + offset + field_offset, f.size);
          std::cout << " " << std::left << std::setw(column_widths[j])
                    << field_data;
          field_offset += f.size;
        }
        std::cout << " |" << std::endl;
      }
      offset += record_size;
    }
  }

  std::cout << separator << std::endl;
}

void SGBD::createOrReplaceRelationFromCSV_fix(const std::string &relation_name,
                                              const std::string &csv_path) {
  std::ifstream file(csv_path);
  if (!file.is_open()) {
    std::cerr << "No se pudo abrir el archivo CSV: " << csv_path << std::endl;
    return;
  }

  std::string line;

  if (!std::getline(file, line)) {
    std::cerr << "CSV vacío o mal formato." << std::endl;
    return;
  }

  std::vector<std::string> type_size_tokens = parseCSVLine(line);
  std::vector<std::string> types;
  std::vector<int> sizes;

  for (const auto &token : type_size_tokens) {
    std::istringstream iss(token);
    std::string t;
    int sz;
    if (!(iss >> t >> sz)) {
      std::cerr << "Error al parsear tipo y tamaño: " << token << std::endl;
      return;
    }
    types.push_back(trim(t));
    sizes.push_back(sz);
  }

  if (!std::getline(file, line)) {
    std::cerr << "No hay línea de nombres de campos." << std::endl;
    return;
  }
  std::vector<std::string> field_names = parseCSVLine(line);

  if (field_names.size() != sizes.size()) {
    std::cerr << "Cantidad de campos y tamaños no coincide." << std::endl;
    return;
  }

  std::vector<Field> fields;
  for (size_t i = 0; i < field_names.size(); ++i) {
    fields.push_back(Field{trim(field_names[i]), types[i], sizes[i]});
  }
  createOrReplaceRelation(relation_name, true, fields);

  while (std::getline(file, line)) {
    if (line.empty())
      continue;
    std::vector<std::string> values = parseCSVLine(line);

    if (values.size() != field_names.size()) {
      std::cerr << "Registro con cantidad de campos incorrecta, ignorado."
                << std::endl;
      continue;
    }

    std::vector<char> record;
    for (size_t i = 0; i < values.size(); ++i) {
      const std::string &val = trim(values[i]);
      int field_size = sizes[i];
      for (int j = 0; j < field_size; ++j) {
        char c = (j < (int)val.size()) ? val[j] : ' ';
        record.push_back(c);
      }
    }

    if (!insert(relation_name, record)) {
      std::cerr << "Error insertando registro en la relación." << std::endl;
      return;
    }
  }
}

bool SGBD::deleteRelation(const std::string &name) {
  if (catalog.hasRelation(name)) {
    const Relation &oldRel = catalog.getRelation(name);
    for (int block : oldRel.blocks) {
      bitmap.set(block, false);
    }
    bitmap.save();
    catalog.removeRelation(name);
    catalog.save();
    return true;
  }
  return false;
}

void SGBD::printRelationSchema(const std::string &relation_name) {
  std::cout << std::endl;
  const Relation &rel = catalog.getRelation(relation_name);
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

void SGBD::printRelation(const std::string &relation_name) {
  const Relation rel = catalog.getRelation(relation_name);
  if (rel.is_fixed) {
    printRelation_fix(relation_name);
  } else {
    // TODO: printRelation_var;
  }
}

bool SGBD::insert(const std::string &relation_name, const std::vector<char> &record) {
  Relation &rel = catalog.getRelation(relation_name);
  if(rel.is_fixed) {
    return insert_fix(rel, record);
  } else {
    //TODO: insert_var();
    return false;
  }
}


