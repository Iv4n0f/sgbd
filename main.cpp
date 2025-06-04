#include "shell.h"

int main() {
  Disk disk("disk", "disk.cfg");
  SGBD sgbd(disk);
  Shell shell(sgbd);
  shell.run();
  return 0;
}
