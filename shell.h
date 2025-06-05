#pragma once

#include "sgbd.h"
#include <string>

class Shell {
  public:
    SGBD &sgbd;
    Shell(SGBD &sgbd);
    void run();
  private:
    bool handleCommand(const std::string &line);
};
