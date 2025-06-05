#pragma once

#include "bitmap.h"
#include "catalog.h"
#include "disk.h"
#include <iostream>

class SGBD {
public:
  static constexpr int HEADER_SIZE_FIX = 16;

  Disk &disk;
  Bitmap bitmap;
  Catalog catalog;

  SGBD(Disk &disk_);

  void printStatus() const;

  void createOrReplaceRelation(const std::string &name, bool is_fixed,
                               const std::vector<Field> &fields);
  bool deleteRelation(const std::string &name);
  void printRelation(const std::string &relation_name);
  void printRelBlockInfo(const std::string &relation_name);

  void initializeBlockHeader_fix(int block_idx, int record_size);

  bool insertRecord_fix(int block_idx, const std::vector<char> &record);
  bool insert_fix(Relation &rel, const std::vector<char> &record);

  bool insert(const std::string &relation_name,
              const std::vector<char> &record);

  void insertFromShell(const std::string &relation_name,
                       const std::vector<std::string> &values);

  void insertFromShell_fix(const std::string &relation_name,
                           const std::vector<std::string> &values);
  void printRelation_fix(const std::string &relation_name);
  void selectWhere_fix(const std::string &relation_name,
                       const std::string &field_name, const std::string &value,
                       const std::string &op,
                       const std::string &output_name = "temp_result");
  void createOrReplaceRelationFromCSV_fix(const std::string &relation_name,
                                          const std::string &csv_path);

  void printRelationSchema(const std::string &relation_name);
  void printDiskCapacityInfo();
};
