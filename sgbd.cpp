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
    initializeBlockHeader_var(block);
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

void SGBD::initializeBlockHeader_var(int block_idx) {
  int number_of_records = 0;
  int end_of_freespace = disk.block_size;

  std::vector<char> block_data(disk.block_size, 0);

  std::string header_str =
      intTo4CharStr(number_of_records) + intTo4CharStr(end_of_freespace);

  std::copy(header_str.begin(), header_str.end(), block_data.begin());

  disk.writeBlockByIndex(block_idx, block_data);
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

bool SGBD::insertRecord_var(int block_idx, const std::vector<char> &record) {
  std::vector<char> block = disk.readBlockByIndex(block_idx);

  int num_records = std::stoi(std::string(block.begin(), block.begin() + 4));
  int end_of_freespace =
      std::stoi(std::string(block.begin() + 4, block.begin() + 8));
  int record_size = record.size();

  int total_required = 8 + record_size;
  int slot_table_end = 8 + num_records * 8;

  if (end_of_freespace - total_required < slot_table_end) {
    return false; // no hay espacio suficiente
  }

  int new_offset = end_of_freespace - record_size;

  std::copy(record.begin(), record.end(), block.begin() + new_offset);

  std::string offset_str = intTo4CharStr(new_offset);
  std::string size_str = intTo4CharStr(record_size);

  std::copy(offset_str.begin(), offset_str.end(),
            block.begin() + slot_table_end);
  std::copy(size_str.begin(), size_str.end(),
            block.begin() + slot_table_end + 4);

  num_records++;
  end_of_freespace = new_offset;

  std::string new_num = intTo4CharStr(num_records);
  std::string new_eof = intTo4CharStr(end_of_freespace);

  std::copy(new_num.begin(), new_num.end(), block.begin());
  std::copy(new_eof.begin(), new_eof.end(), block.begin() + 4);

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

bool SGBD::insert_var(Relation &rel, const std::vector<char> &record) {
  if (rel.is_fixed) {
    std::cerr << "La relación '" << rel.name << "' no es de registros variables"
              << std::endl;
    return false;
  }

  for (int block_idx : rel.blocks) {
    if (insertRecord_var(block_idx, record)) {
      return true;
    }
  }

  int new_block = bitmap.getFreeBlock();
  if (new_block == -1) {
    std::cerr << "No hay bloques libres disponibles para insertar" << std::endl;
    return false;
  }

  bitmap.set(new_block, true);
  initializeBlockHeader_var(new_block); // La que inicializa bloques variables

  if (!insertRecord_var(new_block, record)) {
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

  int total_width = 3;
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

void SGBD::printRelation_var(const std::string &relation_name) {
  std::cout << std::endl;
  if (!catalog.hasRelation(relation_name)) {
    std::cout << "Relación no encontrada: " << relation_name << std::endl;
    return;
  }

  const Relation &rel = catalog.getRelation(relation_name);
  if (rel.is_fixed) {
    std::cout << "La relación no es de tamaño variable." << std::endl;
    return;
  }

  std::vector<int> column_widths;
  for (const auto &f : rel.fields) {
    column_widths.push_back((int)f.name.size());
  }

  // PRIMERA PASADA: Calcular tamaños máximos de cada columna
  for (int block_idx : rel.blocks) {
    std::vector<char> block = disk.readBlockByIndex(block_idx);
    int num_records = std::stoi(std::string(block.begin(), block.begin() + 4));
    int metadata_start = HEADER_SIZE_VAR;

    for (int i = 0; i < num_records; ++i) {
      int entry_offset = metadata_start + i * 8;
      int record_offset = std::stoi(std::string(
          block.begin() + entry_offset, block.begin() + entry_offset + 4));
      if (record_offset == -1)
        continue;

      std::string reg_header(block.begin() + record_offset,
                             block.begin() + record_offset +
                                 rel.fields.size() * 6);

      for (size_t j = 0; j < rel.fields.size(); ++j) {
        int off_idx = j * 6;
        int field_rel_offset = std::stoi(std::string(
            reg_header.begin() + off_idx, reg_header.begin() + off_idx + 3));
        int field_length =
            std::stoi(std::string(reg_header.begin() + off_idx + 3,
                                  reg_header.begin() + off_idx + 6));

        int absolute_offset =
            record_offset + rel.fields.size() * 6 + field_rel_offset;
        std::string field_data(block.begin() + absolute_offset,
                               block.begin() + absolute_offset + field_length);

        column_widths[j] = std::max(column_widths[j], (int)field_data.size());
      }
    }
  }

  // Imprimir encabezado
  int total_width = 3;
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

  // SEGUNDA PASADA: Imprimir datos
  for (int block_idx : rel.blocks) {
    std::vector<char> block = disk.readBlockByIndex(block_idx);
    int num_records = std::stoi(std::string(block.begin(), block.begin() + 4));
    int metadata_start = HEADER_SIZE_VAR;

    for (int i = 0; i < num_records; ++i) {
      int entry_offset = metadata_start + i * 8;
      int record_offset = std::stoi(std::string(
          block.begin() + entry_offset, block.begin() + entry_offset + 4));
      if (record_offset == -1)
        continue;

      int record_header_size = rel.fields.size() * 6;
      std::string reg_header(block.begin() + record_offset,
                             block.begin() + record_offset +
                                 record_header_size);

      std::vector<std::string> campos;
      for (size_t j = 0; j < rel.fields.size(); ++j) {
        int off_idx = j * 6;
        int field_rel_offset = std::stoi(std::string(
            reg_header.begin() + off_idx, reg_header.begin() + off_idx + 3));
        int field_length =
            std::stoi(std::string(reg_header.begin() + off_idx + 3,
                                  reg_header.begin() + off_idx + 6));

        int absolute_offset =
            record_offset + record_header_size + field_rel_offset;
        std::string field_data(block.begin() + absolute_offset,
                               block.begin() + absolute_offset + field_length);
        campos.push_back(field_data);
      }

      std::cout << "|";
      for (size_t j = 0; j < campos.size(); ++j) {
        std::cout << " " << std::left << std::setw(column_widths[j])
                  << campos[j];
      }
      std::cout << " |" << std::endl;
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

void SGBD::createOrReplaceRelationFromCSV_var(const std::string &relation_name,
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

  for (const auto &token : type_size_tokens) {
    std::istringstream iss(token);
    std::string t;
    int dummy;
    if (!(iss >> t >> dummy)) {
      std::cerr << "Error al parsear tipo y tamaño: " << token << std::endl;
      return;
    }
    types.push_back(trim(t));
  }

  if (!std::getline(file, line)) {
    std::cerr << "No hay línea de nombres de campos." << std::endl;
    return;
  }

  std::vector<std::string> field_names = parseCSVLine(line);
  if (field_names.size() != types.size()) {
    std::cerr << "Cantidad de campos y tipos no coincide." << std::endl;
    return;
  }

  std::vector<Field> fields;
  for (size_t i = 0; i < field_names.size(); ++i) {
    fields.push_back(Field{trim(field_names[i]), types[i], -1});
  }

  createOrReplaceRelation(relation_name, false, fields);

  while (std::getline(file, line)) {
    if (line.empty())
      continue;

    std::vector<std::string> values = parseCSVLine(line);
    if (values.size() != field_names.size()) {
      std::cerr << "Registro con cantidad de campos incorrecta, ignorado."
                << std::endl;
      continue;
    }

    std::vector<std::string> trimmed_fields;
    for (auto &v : values)
      trimmed_fields.push_back(trim(v));

    int current_relative_offset = 0;

    std::vector<char> record;
    std::ostringstream header_stream;

    for (const auto &field : trimmed_fields) {
      std::ostringstream off, len;
      off << std::setw(3) << std::setfill('0') << current_relative_offset;
      len << std::setw(3) << std::setfill('0') << field.size();

      header_stream << off.str() << len.str();
      current_relative_offset += field.size();
    }

    std::string header = header_stream.str();
    record.insert(record.end(), header.begin(), header.end());

    for (const auto &field : trimmed_fields) {
      record.insert(record.end(), field.begin(), field.end());
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
  std::cout << "Relacion a borrar no encontrada: " << name << std::endl;
  return false;
}

void SGBD::printRelationSchema(const std::string &relation_name) {
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
}

void SGBD::printRelation(const std::string &relation_name) {
  const Relation &rel = catalog.getRelation(relation_name);
  if (rel.is_fixed) {
    printRelation_fix(relation_name);
  } else {
    printRelation_var(relation_name);
  }
}

bool SGBD::insert(const std::string &relation_name,
                  const std::vector<char> &record) {
  Relation &rel = catalog.getRelation(relation_name);
  if (rel.is_fixed) {
    return insert_fix(rel, record);
  } else {
    return insert_var(rel, record);
  }
}

bool compareValues(const std::string &op, int v1, int v2) {
  if (op == "==")
    return v1 == v2;
  if (op == "!=")
    return v1 != v2;
  if (op == "<")
    return v1 < v2;
  if (op == "<=")
    return v1 <= v2;
  if (op == ">")
    return v1 > v2;
  if (op == ">=")
    return v1 >= v2;
  std::cout << "Operador no válido: " << op << std::endl;
  return false;
}

bool compareValues(const std::string &op, float v1, float v2) {
  if (op == "==")
    return v1 == v2;
  if (op == "!=")
    return v1 != v2;
  if (op == "<")
    return v1 < v2;
  if (op == "<=")
    return v1 <= v2;
  if (op == ">")
    return v1 > v2;
  if (op == ">=")
    return v1 >= v2;
  std::cout << "Operador no válido: " << op << std::endl;
  return false;
}

bool compareValues(const std::string &op, const std::string &v1,
                   const std::string &v2) {
  if (op == "==")
    return v1 == v2;
  if (op == "!=")
    return v1 != v2;
  if (op == "<")
    return v1 < v2;
  if (op == "<=")
    return v1 <= v2;
  if (op == ">")
    return v1 > v2;
  if (op == ">=")
    return v1 >= v2;
  std::cout << "Operador no válido: " << op << std::endl;
  return false;
}

bool stringToInt(const std::string &s, int &out) {
  char *end = nullptr;
  long val = std::strtol(s.c_str(), &end, 10);
  if (end != s.c_str() && *end == '\0') {
    out = static_cast<int>(val);
    return true;
  }
  return false;
}

bool stringToFloat(const std::string &s, float &out) {
  char *end = nullptr;
  float val = std::strtof(s.c_str(), &end);
  if (end != s.c_str() && *end == '\0') {
    out = val;
    return true;
  }
  return false;
}

void SGBD::selectWhere_fix(const std::string &relation_name,
                           const std::string &field_name,
                           const std::string &value, const std::string &op,
                           const std::string &output_name) {
  if (!catalog.hasRelation(relation_name)) {
    std::cout << "Relación no encontrada: " << relation_name << std::endl;
    return;
  }

  const Relation &input_rel = catalog.getRelation(relation_name);
  if (!input_rel.is_fixed) {
    std::cout << "Solo se soportan relaciones de tamaño fijo, llamar a "
                 "selectWhere_var()"
              << std::endl;
    return;
  }

  int field_idx = -1, offset = 0;
  for (size_t i = 0; i < input_rel.fields.size(); ++i) {
    if (input_rel.fields[i].name == field_name) {
      field_idx = i;
      break;
    }
    offset += input_rel.fields[i].size;
  }

  if (field_idx == -1) {
    std::cout << "Campo no encontrado: " << field_name << std::endl;
    return;
  }

  createOrReplaceRelation(output_name, true, input_rel.fields);

  int record_size = calculateRecordSize(input_rel.fields);
  const std::string &field_type = input_rel.fields[field_idx].type;

  for (int block_idx : input_rel.blocks) {
    std::vector<char> block = disk.readBlockByIndex(block_idx);

    int free_list_head =
        std::stoi(std::string(block.begin(), block.begin() + 4));
    int record_size_header =
        std::stoi(std::string(block.begin() + 4, block.begin() + 8));
    int active_records =
        std::stoi(std::string(block.begin() + 12, block.begin() + 16));

    if (record_size_header != record_size) {
      std::cout << "Record size no coincide, saltando bloque ... ERROR critico"
                << std::endl;
      continue;
    }

    std::unordered_set<int> deleted;
    int current = free_list_head;
    while (current != -1) {
      deleted.insert(current);
      int reg_offset = HEADER_SIZE_FIX + current * record_size;
      int next = std::stoi(std::string(block.begin() + reg_offset,
                                       block.begin() + reg_offset + 4));
      current = next;
    }

    int total = active_records + deleted.size();
    int pos = HEADER_SIZE_FIX;

    for (int i = 0; i < total; ++i) {
      if (deleted.count(i)) {
        pos += record_size;
        continue;
      }

      std::string field_val(block.begin() + pos + offset,
                            block.begin() + pos + offset +
                                input_rel.fields[field_idx].size);
      field_val = trim(field_val);

      bool match = false;

      if (field_type == "int") {
        int field_num, value_num;
        if (!stringToInt(field_val, field_num) ||
            !stringToInt(value, value_num)) {
          pos += record_size;
          continue;
        }
        match = compareValues(op, field_num, value_num);

      } else if (field_type == "float") {
        float field_num, value_num;
        if (!stringToFloat(field_val, field_num) ||
            !stringToFloat(value, value_num)) {
          pos += record_size;
          continue;
        }
        match = compareValues(op, field_num, value_num);

      } else if (field_type == "string") {
        match = compareValues(op, field_val, value);

      } else {
        std::cout << "Tipo de campo no soportado: " << field_type << std::endl;
        pos += record_size;
        continue;
      }

      if (match) {
        std::vector<char> reg(block.begin() + pos,
                              block.begin() + pos + record_size);
        insert(output_name, reg);
      }

      pos += record_size;
    }
  }

  printRelation(output_name);

  if (output_name == "temp_result") {
    deleteRelation(output_name);
  }
}

void SGBD::selectWhere_var(const std::string &relation_name,
                           const std::string &field_name,
                           const std::string &value, const std::string &op,
                           const std::string &output_name) {
  if (!catalog.hasRelation(relation_name)) {
    std::cout << "Relación no encontrada: " << relation_name << std::endl;
    return;
  }

  const Relation &input_rel = catalog.getRelation(relation_name);
  if (input_rel.is_fixed) {
    std::cout << "La relación es de tamaño fijo. Llamar a selectWhere_fix()."
              << std::endl;
    return;
  }

  int field_idx = -1;
  for (size_t i = 0; i < input_rel.fields.size(); ++i) {
    if (input_rel.fields[i].name == field_name) {
      field_idx = i;
      break;
    }
  }

  if (field_idx == -1) {
    std::cout << "Campo no encontrado: " << field_name << std::endl;
    return;
  }

  createOrReplaceRelation(output_name, false, input_rel.fields);
  const std::string &field_type = input_rel.fields[field_idx].type;

  for (int block_idx : input_rel.blocks) {
    std::vector<char> block = disk.readBlockByIndex(block_idx);

    int total_records =
        std::stoi(std::string(block.begin(), block.begin() + 4));

    for (int i = 0; i < total_records; ++i) {
      int entry_offset = 8 + i * 8;

      int reg_offset = std::stoi(std::string(block.begin() + entry_offset,
                                             block.begin() + entry_offset + 4));
      int reg_size = std::stoi(std::string(block.begin() + entry_offset + 4,
                                           block.begin() + entry_offset + 8));

      if (reg_offset == -1)
        continue; // Registro eliminado

      int reg_start = reg_offset;

      std::vector<std::pair<int, int>> campo_offsets;
      for (size_t j = 0; j < input_rel.fields.size(); ++j) {
        int local = reg_start + j * 6;

        int off = std::stoi(
            std::string(block.begin() + local, block.begin() + local + 3));
        int len = std::stoi(
            std::string(block.begin() + local + 3, block.begin() + local + 6));
        campo_offsets.emplace_back(off, len);
      }

      int header_size = input_rel.fields.size() * 6;
      int field_rel_offset = campo_offsets[field_idx].first;
      int field_len = campo_offsets[field_idx].second;
      int field_abs_offset = reg_start + header_size + field_rel_offset;

      std::string field_val(block.begin() + field_abs_offset,
                            block.begin() + field_abs_offset + field_len);
      field_val = trim(field_val);

      bool match = false;

      if (field_type == "int") {
        int field_num, value_num;
        if (!stringToInt(field_val, field_num) ||
            !stringToInt(value, value_num)) {
          continue;
        }
        match = compareValues(op, field_num, value_num);

      } else if (field_type == "float") {
        float field_num, value_num;
        if (!stringToFloat(field_val, field_num) ||
            !stringToFloat(value, value_num)) {
          continue;
        }
        match = compareValues(op, field_num, value_num);

      } else if (field_type == "string") {
        match = compareValues(op, field_val, value);

      } else {
        std::cout << "Tipo de campo no soportado: " << field_type << std::endl;
        continue;
      }

      if (match) {
        std::vector<char> registro(block.begin() + reg_offset,
                                   block.begin() + reg_offset + reg_size);
        insert(output_name, registro);
      }
    }
  }

  printRelation(output_name);

  if (output_name == "temp_result") {
    deleteRelation(output_name);
  }
}

void SGBD::selectWhere(const std::string &relation_name,
                       const std::string &field_name, const std::string &value,
                       const std::string &op, const std::string &output_name) {
  if (!catalog.hasRelation(relation_name)) {
    std::cout << "Relación no encontrada: " << relation_name << std::endl;
    return;
  }

  const Relation &rel = catalog.getRelation(relation_name);

  if (rel.is_fixed) {
    selectWhere_fix(relation_name, field_name, value, op, output_name);
  } else {
    selectWhere_var(relation_name, field_name, value, op, output_name);
  }
}

void SGBD::printRelBlockInfo(const std::string &relation_name) {
  const Relation &rel = catalog.getRelation(relation_name);
  std::cout << "\nBloques de la relación '" << rel.name << "':\n";

  for (int block_idx : rel.blocks) {
    std::vector<char> block = disk.readBlockByIndex(block_idx);

    int used_bytes = 0;

    if (rel.is_fixed) {
      int record_size =
          std::stoi(std::string(block.begin() + 4, block.begin() + 8));
      int active_records =
          std::stoi(std::string(block.begin() + 12, block.begin() + 16));
      used_bytes = HEADER_SIZE_FIX + record_size * active_records;
    } else {
      int total_records =
          std::stoi(std::string(block.begin(), block.begin() + 4));
      int free_space_offset =
          std::stoi(std::string(block.begin() + 4, block.begin() + 8));
      int data_bytes = disk.block_size - free_space_offset;
      used_bytes = HEADER_SIZE_VAR + 8 * total_records + data_bytes;
    }

    std::cout << "Bloque " << block_idx
              << " | Posición física: " << disk.getBlockPosition(block_idx)
              << " | Bytes ocupados: " << used_bytes << " / " << disk.block_size
              << '\n';
  }

  std::cout << std::endl;
}

void SGBD::insertFromShell_fix(const std::string &relation_name,
                               const std::vector<std::string> &values) {
  Relation &rel = catalog.getRelation(relation_name);
  if (!rel.is_fixed) {
    std::cerr << "Error: la relación '" << relation_name
              << "' no es de registros fijos." << std::endl;
    return;
  }

  if ((int)values.size() != (int)rel.fields.size()) {
    std::cerr << "Error: número de valores no coincide con el número de campos."
              << std::endl;
    return;
  }

  std::vector<char> record;
  for (size_t i = 0; i < rel.fields.size(); ++i) {
    int len = rel.fields[i].size;
    std::string val = values[i];

    if ((int)val.size() > len) {
      std::cerr << "Error: valor '" << val << "' excede el tamaño del campo '"
                << rel.fields[i].name << "'." << std::endl;
      return;
    }

    std::string padded = val + std::string(len - val.size(), ' ');
    record.insert(record.end(), padded.begin(), padded.end());
  }

  for (int block_idx : rel.blocks) {
    if (insertRecord_fix(block_idx, record)) {
      disk.printBlockPosition(block_idx);
      return;
    }
  }

  int new_block = bitmap.getFreeBlock();
  if (new_block == -1) {
    std::cerr << "Error: no hay bloques libres." << std::endl;
    return;
  }

  bitmap.set(new_block, true);
  initializeBlockHeader_fix(new_block, calculateRecordSize(rel.fields));

  if (!insertRecord_fix(new_block, record)) {
    std::cerr << "Error crítico al insertar en nuevo bloque." << std::endl;
    return;
  }

  rel.blocks.push_back(new_block);
  bitmap.save();
  catalog.save();

  disk.printBlockPosition(new_block);
}

void SGBD::insertFromShell_var(const std::string &relation_name,
                               const std::vector<std::string> &values) {
  Relation &rel = catalog.getRelation(relation_name);
  if (rel.is_fixed) {
    std::cerr << "Error: la relación '" << relation_name
              << "' no es de registros variables." << std::endl;
    return;
  }

  if ((int)values.size() != (int)rel.fields.size()) {
    std::cerr << "Error: número de valores no coincide con el número de campos."
              << std::endl;
    return;
  }

  std::vector<std::string> trimmed_values;
  for (const auto &val : values)
    trimmed_values.push_back(trim(val));

  std::ostringstream header_stream;
  int current_relative_offset = 0;

  for (size_t i = 0; i < trimmed_values.size(); ++i) {
    const std::string &field = trimmed_values[i];

    int offset = current_relative_offset;
    int length = field.size();
    current_relative_offset += length;

    header_stream << std::setw(3) << std::setfill('0') << offset;
    header_stream << std::setw(3) << std::setfill('0') << length;
  }

  std::string header = header_stream.str();
  std::vector<char> record(header.begin(), header.end());

  for (const std::string &field : trimmed_values)
    record.insert(record.end(), field.begin(), field.end());

  for (int block_idx : rel.blocks) {
    if (insertRecord_var(block_idx, record)) {
      disk.printBlockPosition(block_idx);
      return;
    }
  }

  int new_block = bitmap.getFreeBlock();
  if (new_block == -1) {
    std::cerr << "Error: no hay bloques libres disponibles." << std::endl;
    return;
  }

  bitmap.set(new_block, true);
  initializeBlockHeader_var(new_block);

  if (!insertRecord_var(new_block, record)) {
    std::cerr << "Error crítico: no se pudo insertar ni en nuevo bloque."
              << std::endl;
    return;
  }

  rel.blocks.push_back(new_block);
  bitmap.save();
  catalog.save();

  disk.printBlockPosition(new_block);
}

void SGBD::insertFromShell(const std::string &relation_name,
                           const std::vector<std::string> &values) {
  Relation &rel = catalog.getRelation(relation_name);
  if (rel.is_fixed) {
    insertFromShell_fix(relation_name, values);
  } else {
    insertFromShell_var(relation_name, values);
  }
}

void SGBD::printDiskCapacityInfo() {
  int total_blocks = bitmap.size();
  int block_size = disk.block_size;

  int reserved_blocks = 2; // Bloques 0 y 1 (bitmap y catálogo)
  int free_blocks = 0;
  int used_blocks = 0;
  int data_blocks = 0;
  int bytes_used_in_data = 0;

  // Contar bloques libres y usados (excluyendo reservados)
  for (int i = reserved_blocks; i < total_blocks; ++i) {
    if (!bitmap.get(i)) {
      free_blocks++;
    } else {
      used_blocks++;
    }
  }

  // Agregar los reservados
  used_blocks += reserved_blocks;

  // Procesar bloques con datos de relaciones
  for (const auto &pair : catalog.getAllRelations()) {
    const Relation &rel = pair.second;

    for (int block_idx : rel.blocks) {
      data_blocks++;

      std::vector<char> block = disk.readBlockByIndex(block_idx);

      if (rel.is_fixed) {
        int record_size =
            std::stoi(std::string(block.begin() + 4, block.begin() + 8));
        int active_records =
            std::stoi(std::string(block.begin() + 12, block.begin() + 16));

        bytes_used_in_data += record_size * active_records + HEADER_SIZE_FIX;
      } else {
        int total_records =
            std::stoi(std::string(block.begin(), block.begin() + 4));
        int free_space_offset =
            std::stoi(std::string(block.begin() + 4, block.begin() + 8));
        int used_data_bytes = block_size - free_space_offset;

        bytes_used_in_data +=
            used_data_bytes + HEADER_SIZE_VAR + 8 * total_records;
      }
    }
  }

  int total_capacity = total_blocks * block_size;
  int free_capacity = free_blocks * block_size;
  int used_capacity = used_blocks * block_size;
  int data_blocks_capacity = data_blocks * block_size;
  int sector_data_capacity = bytes_used_in_data;

  std::cout << "Cantidad total de bloques: " << total_blocks << "\n";
  std::cout << "Capacidad total del disco: " << total_capacity << " bytes\n";
  std::cout << "Capacidad libre: " << free_capacity << " bytes\n";
  std::cout << "Capacidad ocupada: " << used_capacity << " bytes\n";
  std::cout << "Capacidad de bloques con datos (relaciones): "
            << data_blocks_capacity << " bytes\n";
  std::cout << "Capacidad usada en sectores con datos (registros activos): "
            << sector_data_capacity << " bytes\n";
}

void SGBD::insertNFromCSV_fix(const std::string &relation_name,
                              const std::string &csv_path, int N) {

  if (!catalog.hasRelation(relation_name)) {
    std::cerr << "La relación '" << relation_name << "' no existe."
              << std::endl;
    return;
  }

  const Relation &rel = catalog.getRelation(relation_name);
  if (!rel.is_fixed) {
    std::cerr << "La relación no es de tamaño fijo." << std::endl;
    return;
  }

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

  if (!std::getline(file, line)) {
    std::cerr << "No hay línea de nombres de campos." << std::endl;
    return;
  }

  const std::vector<Field> &fields = rel.fields;
  int inserted = 0;

  while (inserted < N && std::getline(file, line)) {
    if (line.empty())
      continue;

    std::vector<std::string> values = parseCSVLine(line);
    if (values.size() != fields.size()) {
      std::cerr << "Registro con cantidad de campos incorrecta, ignorado."
                << std::endl;
      continue;
    }

    std::vector<char> record;
    for (size_t i = 0; i < values.size(); ++i) {
      std::string val = trim(values[i]);
      int field_size = fields[i].size;
      for (int j = 0; j < field_size; ++j) {
        char c = (j < (int)val.size()) ? val[j] : ' ';
        record.push_back(c);
      }
    }

    if (!insert(relation_name, record)) {
      std::cerr << "Error insertando registro en la relación." << std::endl;
      return;
    }

    ++inserted;
  }
}

void SGBD::insertNFromCSV_var(const std::string &relation_name,
                              const std::string &csv_path, int N) {
  if (!catalog.hasRelation(relation_name)) {
    std::cerr << "La relación '" << relation_name << "' no existe."
              << std::endl;
    return;
  }

  const Relation &rel = catalog.getRelation(relation_name);
  if (rel.is_fixed) {
    std::cerr << "La relación no es de tamaño variable." << std::endl;
    return;
  }

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

  if (!std::getline(file, line)) {
    std::cerr << "No hay línea de nombres de campos." << std::endl;
    return;
  }

  int count = 0;
  while (count < N && std::getline(file, line)) {
    if (line.empty())
      continue;

    std::vector<std::string> values = parseCSVLine(line);
    if ((int)values.size() != (int)rel.fields.size()) {
      std::cerr << "Registro con cantidad de campos incorrecta, ignorado."
                << std::endl;
      continue;
    }

    std::vector<std::string> trimmed_fields;
    for (auto &v : values)
      trimmed_fields.push_back(trim(v));

    int current_relative_offset = 0;
    std::vector<char> record;
    std::ostringstream header_stream;

    for (const auto &field : trimmed_fields) {
      std::ostringstream off, len;
      off << std::setw(3) << std::setfill('0') << current_relative_offset;
      len << std::setw(3) << std::setfill('0') << field.size();
      header_stream << off.str() << len.str();
      current_relative_offset += field.size();
    }

    std::string header = header_stream.str();
    record.insert(record.end(), header.begin(), header.end());

    for (const auto &field : trimmed_fields) {
      record.insert(record.end(), field.begin(), field.end());
    }

    if (!insert(relation_name, record)) {
      std::cerr << "Error insertando registro en la relación." << std::endl;
      return;
    }

    ++count;
  }
}

void SGBD::insertNFromCSV(const std::string &relation_name,
                          const std::string &csv_path, int N) {
  Relation &rel = catalog.getRelation(relation_name);

  if (rel.is_fixed) {
    insertNFromCSV_fix(relation_name, csv_path, N);
  } else {
    insertNFromCSV_var(relation_name, csv_path, N);
  }
}

void SGBD::deleteWhere_fix(const std::string &relation_name,
                           const std::string &field_name,
                           const std::string &value, const std::string &op) {
  if (!catalog.hasRelation(relation_name)) {
    std::cout << "Relación no encontrada: " << relation_name << std::endl;
    return;
  }

  const Relation &rel = catalog.getRelation(relation_name);

  int field_idx = -1, offset = 0;
  for (size_t i = 0; i < rel.fields.size(); ++i) {
    if (rel.fields[i].name == field_name) {
      field_idx = i;
      break;
    }
    offset += rel.fields[i].size;
  }

  if (field_idx == -1) {
    std::cout << "Campo no encontrado: " << field_name << std::endl;
    return;
  }

  int record_size = calculateRecordSize(rel.fields);
  const std::string &field_type = rel.fields[field_idx].type;

  for (int block_idx : rel.blocks) {
    std::vector<char> block = disk.readBlockByIndex(block_idx);

    int free_list_head =
        std::stoi(std::string(block.begin(), block.begin() + 4));
    int record_size_header =
        std::stoi(std::string(block.begin() + 4, block.begin() + 8));
    int active_records =
        std::stoi(std::string(block.begin() + 12, block.begin() + 16));

    if (record_size_header != record_size) {
      std::cout << "Record size no coincide, saltando bloque... (ERROR crítico)"
                << std::endl;
      continue;
    }

    std::unordered_set<int> deleted;
    int current = free_list_head;
    while (current != -1) {
      deleted.insert(current);
      int reg_offset = HEADER_SIZE_FIX + current * record_size;
      int next = std::stoi(std::string(block.begin() + reg_offset,
                                       block.begin() + reg_offset + 4));
      current = next;
    }

    int total = active_records + deleted.size();
    int pos = HEADER_SIZE_FIX;
    bool modified = false;

    for (int i = 0; i < total; ++i) {
      if (deleted.count(i)) {
        pos += record_size;
        continue;
      }

      std::string field_val(block.begin() + pos + offset,
                            block.begin() + pos + offset +
                                rel.fields[field_idx].size);
      field_val = trim(field_val);

      bool match = false;

      if (field_type == "int") {
        int field_num, value_num;
        if (!stringToInt(field_val, field_num) ||
            !stringToInt(value, value_num)) {
          pos += record_size;
          continue;
        }
        match = compareValues(op, field_num, value_num);
      } else if (field_type == "float") {
        float field_num, value_num;
        if (!stringToFloat(field_val, field_num) ||
            !stringToFloat(value, value_num)) {
          pos += record_size;
          continue;
        }
        match = compareValues(op, field_num, value_num);
      } else if (field_type == "string") {
        match = compareValues(op, field_val, value);
      } else {
        std::cout << "Tipo no soportado: " << field_type << std::endl;
        pos += record_size;
        continue;
      }

      if (match) {
        int reg_offset = HEADER_SIZE_FIX + i * record_size;

        // escribir el antiguo free_list_head como "next" del nuevo eliminado
        std::string next_str = intTo4CharStr(free_list_head);
        std::copy(next_str.begin(), next_str.end(), block.begin() + reg_offset);

        // actualizar el free_list_head
        free_list_head = i;
        std::string head_str = intTo4CharStr(free_list_head);
        std::copy(head_str.begin(), head_str.end(), block.begin());

        // actualizar active_records
        active_records--;
        std::string new_active_str = intTo4CharStr(active_records);
        std::copy(new_active_str.begin(), new_active_str.end(),
                  block.begin() + 12);

        modified = true;
      }

      pos += record_size;
    }

    if (modified) {
      disk.writeBlockByIndex(block_idx, block);
      std::cout << "Ubicacion del registro eliminado" << std::endl;
      disk.printBlockPosition(block_idx);
    }
  }
}

void SGBD::deleteWhere_var(const std::string &relation_name,
                           const std::string &field_name,
                           const std::string &value,
                           const std::string &op) {
  if (!catalog.hasRelation(relation_name)) {
    std::cout << "Relación no encontrada: " << relation_name << std::endl;
    return;
  }

  const Relation &rel = catalog.getRelation(relation_name);

  int field_idx = -1;
  for (size_t i = 0; i < rel.fields.size(); ++i) {
    if (rel.fields[i].name == field_name) {
      field_idx = i;
      break;
    }
  }

  if (field_idx == -1) {
    std::cout << "Campo no encontrado: " << field_name << std::endl;
    return;
  }

  const std::string &field_type = rel.fields[field_idx].type;

  for (int block_idx : rel.blocks) {
    std::vector<char> block = disk.readBlockByIndex(block_idx);

    int total_records = std::stoi(std::string(block.begin(), block.begin() + 4));
    bool modified = false;

    for (int i = 0; i < total_records; ++i) {
      int entry_offset = 8 + i * 8;

      int reg_offset = std::stoi(std::string(block.begin() + entry_offset,
                                             block.begin() + entry_offset + 4));

      if (reg_offset == -1) continue; 

      int reg_start = reg_offset;

      std::vector<std::pair<int, int>> campo_offsets;
      for (size_t j = 0; j < rel.fields.size(); ++j) {
        int local = reg_start + j * 6;

        int off = std::stoi(std::string(block.begin() + local, block.begin() + local + 3));
        int len = std::stoi(std::string(block.begin() + local + 3, block.begin() + local + 6));
        campo_offsets.emplace_back(off, len);
      }

      int header_size = rel.fields.size() * 6;
      int field_rel_offset = campo_offsets[field_idx].first;
      int field_len = campo_offsets[field_idx].second;
      int field_abs_offset = reg_start + header_size + field_rel_offset;

      std::string field_val(block.begin() + field_abs_offset,
                            block.begin() + field_abs_offset + field_len);
      field_val = trim(field_val);

      bool match = false;

      if (field_type == "int") {
        int field_num, value_num;
        if (!stringToInt(field_val, field_num) || !stringToInt(value, value_num)) {
          continue;
        }
        match = compareValues(op, field_num, value_num);
      } else if (field_type == "float") {
        float field_num, value_num;
        if (!stringToFloat(field_val, field_num) || !stringToFloat(value, value_num)) {
          continue;
        }
        match = compareValues(op, field_num, value_num);
      } else if (field_type == "string") {
        match = compareValues(op, field_val, value);
      } else {
        std::cout << "Tipo de campo no soportado: " << field_type << std::endl;
        continue;
      }

      if (match) {
        std::string minus_one = intTo4CharStr(-1);
        std::copy(minus_one.begin(), minus_one.end(), block.begin() + entry_offset);
        modified = true;
      }
    }

    if (modified) {
      disk.writeBlockByIndex(block_idx, block);
      compactBlock_var(block_idx);
    }
  }
}

void SGBD::deleteWhere(const std::string &relation_name,
                       const std::string &field_name,
                       const std::string &value,
                       const std::string &op) {
  if (!catalog.hasRelation(relation_name)) {
    std::cout << "Relación no encontrada: " << relation_name << std::endl;
    return;
  }

  const Relation &rel = catalog.getRelation(relation_name);

  if (rel.is_fixed) {
    deleteWhere_fix(relation_name, field_name, value, op);
  } else {
    deleteWhere_var(relation_name, field_name, value, op);
  }
}

void SGBD::compactBlock_var(int block_idx) {
  std::vector<char> old_block = disk.readBlockByIndex(block_idx);

  int total_records = std::stoi(std::string(old_block.begin(), old_block.begin() + 4));

  std::vector<std::vector<char>> valid_records;
  std::vector<int> valid_sizes;

  for (int i = 0; i < total_records; ++i) {
    int entry_offset = 8 + i * 8;

    int reg_offset = std::stoi(std::string(old_block.begin() + entry_offset,
                                           old_block.begin() + entry_offset + 4));
    int reg_size = std::stoi(std::string(old_block.begin() + entry_offset + 4,
                                         old_block.begin() + entry_offset + 8));

    if (reg_offset == -1) continue; // Registro eliminado

    std::vector<char> registro(old_block.begin() + reg_offset,
                               old_block.begin() + reg_offset + reg_size);
    valid_records.push_back(registro);
    valid_sizes.push_back(reg_size);
  }

  std::vector<char> new_block(disk.block_size, 0);
  int new_num_records = valid_records.size();
  int eof = disk.block_size;
  int slot_ptr = 8;

  for (size_t i = 0; i < valid_records.size(); ++i) {
    const std::vector<char> &reg = valid_records[i];
    int size = valid_sizes[i];
    int new_offset = eof - size;

    std::copy(reg.begin(), reg.end(), new_block.begin() + new_offset);

    std::string offset_str = intTo4CharStr(new_offset);
    std::string size_str = intTo4CharStr(size);

    std::copy(offset_str.begin(), offset_str.end(), new_block.begin() + slot_ptr);
    std::copy(size_str.begin(), size_str.end(), new_block.begin() + slot_ptr + 4);

    slot_ptr += 8;
    eof = new_offset;
  }

  std::string num_str = intTo4CharStr(new_num_records);
  std::string eof_str = intTo4CharStr(eof);

  std::copy(num_str.begin(), num_str.end(), new_block.begin());
  std::copy(eof_str.begin(), eof_str.end(), new_block.begin() + 4);

  disk.writeBlockByIndex(block_idx, new_block);
}
