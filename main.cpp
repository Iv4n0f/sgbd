#include "sgbd.h"

int main() {
  Disk disk("disk", "disk.cfg");

  SGBD sgbd(disk);
  sgbd.createOrReplaceRelationFromCSV_fix("titanic", "titanic.csv");
  sgbd.printRelation_fix("titanic");
  sgbd.printStatus();
  return 0;
}
