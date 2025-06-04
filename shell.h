#pragma once

#include "sgbd.h"
#include <string>

class Shell {
  public:
    Shell(SGBD &sgbd);
    void run();
  private:
    SGBD &sgbd;
    bool handleCommand(const std::string &line);
};
