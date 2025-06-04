#include "shell.h"
#include <cstdio>

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
  } else if (cmd == "schema" && tokens.size() >= 2) {
    sgbd.printRelationSchema(tokens[1]);
  } else if (cmd == "select" && tokens.size() >= 2) {
    if (tokens[1] == "all") {
      if (tokens.size() >= 3) {
        sgbd.printRelation(tokens[2]);
      } else {
        std::cerr << "No se especifico una relacion" << std::endl;
      }
    }
  } else {
    std::cout << "Comando no reconocido." << std::endl;
  }

  return true;
}
