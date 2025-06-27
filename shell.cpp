#include "shell.h"
#include <cstdio>
#include <string>

std::vector<std::string> split(const std::string &line) {
  std::vector<std::string> tokens;
  std::string current;
  bool in_quotes = false;

  for (size_t i = 0; i < line.size(); ++i) {
    char c = line[i];

    if (c == '\"') {
      in_quotes = !in_quotes;
      continue;
    }

    if (std::isspace(c) && !in_quotes) {
      if (!current.empty()) {
        tokens.push_back(current);
        current.clear();
      }
    } else {
      current += c;
    }
  }

  if (!current.empty()) {
    tokens.push_back(current);
  }

  return tokens;
}

Shell::Shell(SGBD &sgbd) : sgbd(sgbd) {}

void Shell::run() {
  std::string line;
  while (true) {
    std::cout << "> ";
    if (!std::getline(std::cin, line))
      break;
    if (!handleCommand(line))
      break;
  }
  sgbd.catalog.save();
  sgbd.bitmap.save();
  sgbd.bufferManager->flushAll();
  std::cout << "Saliendo del sistema..." << std::endl;
}

bool Shell::handleCommand(const std::string &line) {
  auto tokens = split(line);
  if (tokens.empty())
    return true;

  const std::string &cmd = tokens[0];

  if (cmd == "exit") {
    return false;
  }

  if (cmd == "status") {
    sgbd.printStatus();
  } else if (cmd == "schema") {
    if (tokens.size() >= 2)
      sgbd.printRelationSchema(tokens[1]);
    else
      std::cerr << "No se especifico una relacion" << std::endl;
  } else if (cmd == "select" && tokens.size() >= 2) {
    if (tokens[1] == "all") {
      if (tokens.size() >= 3)
        sgbd.printRelation(tokens[2]);
      else
        std::cerr << "No se especifico una relacion" << std::endl;
    }
    if (tokens[1] == "where" && tokens.size() >= 6) {
      std::string field = tokens[2];
      std::string op = tokens[3];
      std::string value = tokens[4];
      std::string relation = tokens[5];
      if (tokens.size() >= 8 && tokens[6] == "|") {
        sgbd.selectWhere(relation, field, value, op, tokens[7]);
      } else if (tokens.size() == 7 && tokens[6] == "|") {
        std::cerr << "Error: Falta el nombre de la nueva relacion luego de '|'"
                  << std::endl;
      } else {
        sgbd.selectWhere(relation, field, value, op);
      }
    }
  } else if (cmd == "add_from_csv" && tokens.size() == 4) {
    if (tokens[3] == "fix") {
      sgbd.createOrReplaceRelationFromCSV_fix(tokens[1], tokens[2]);
    } else if (tokens[3] == "var") {
      sgbd.createOrReplaceRelationFromCSV_var(tokens[1], tokens[2]);
    }
  } else if (cmd == "insert_from_csv" && tokens.size() == 4) {
    sgbd.insertNFromCSV(tokens[1], tokens[2], std::stoi(tokens[3]));
  } else if (cmd == "rel_block_info" && tokens.size() == 2) {
    sgbd.printRelBlockInfo(tokens[1]);
  } else if (cmd == "block_info" && tokens.size() == 2) {
    sgbd.disk.printBlockPosition(std::stoi(tokens[1]));
  } else if (cmd == "disk_info" && tokens.size() == 1) {
    sgbd.disk.printDiskInfo();
  } else if (cmd == "disk_cap" && tokens.size() == 1) {
    sgbd.printDiskCapacityInfo();
  } else if (cmd == "delete" && tokens.size() >= 2) {
    if (tokens.size() == 2)
      sgbd.deleteRelation(tokens[1]);
    else if (tokens[1] == "where" && tokens.size() == 6) {
      std::string field = tokens[2];
      std::string op = tokens[3];
      std::string value = tokens[4];
      std::string relation = tokens[5];
      sgbd.deleteWhere(relation, field, value, op);
    }
  } else if (cmd == "insert" && tokens.size() >= 3) {
    std::string relation_name = tokens[1];
    std::vector<std::string> values(tokens.begin() + 2, tokens.end());
    sgbd.insertFromShell(relation_name, values);
  } else if (cmd == "buffer_status") {
    sgbd.bufferManager->printStatus();
  } else if (cmd == "print_block" && tokens.size() == 2) {
    sgbd.printBlock(std::stoi(tokens[1]));
  } else if (cmd == "pin" && tokens.size() == 2) {
    sgbd.bufferManager->pin(std::stoi(tokens[1]));
  } else if (cmd == "unpin" && tokens.size() == 2) {
    sgbd.bufferManager->unpin(std::stoi(tokens[1]));
  } else if (cmd == "request" && tokens.size() == 2) {
    sgbd.bufferManager->getBlock(std::stoi(tokens[1]));
  } else if (cmd == "dirty" && tokens.size() == 2) {
    sgbd.bufferManager->markDirty(std::stoi(tokens[1]));
  } else {
    std::cout << "Comando no reconocido." << std::endl;
  }
  sgbd.bufferManager->printStatus();
  return true;
}
