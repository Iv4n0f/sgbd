#pragma once

#include "bitmap.h"
#include "buffermanager.h"
#include "catalog.h"
#include "disk.h"
#include <iostream>
#include <memory>

class SGBD {
public:
  static constexpr int HEADER_SIZE_FIX = 16;
  static constexpr int HEADER_SIZE_VAR = 8;

  Disk &disk;
  Bitmap bitmap;
  Catalog catalog;
  std::unique_ptr<BufferManager> bufferManager;

  SGBD(Disk &disk_);

  void printStatus() const;

  void createOrReplaceRelation(const std::string &name, bool is_fixed,
                               const std::vector<Field> &fields);
  bool deleteRelation(const std::string &name);
  void printRelBlockInfo(const std::string &relation_name);

  void initializeBlockHeader_fix(int block_idx, int record_size);
  void initializeBlockHeader_var(int block_idx);

  bool insertRecord_fix(int block_idx, const std::vector<char> &record);
  bool insertRecord_var(int block_idx, const std::vector<char> &record);

  bool insert(const std::string &relation_name,
              const std::vector<char> &record);
  bool insert_fix(Relation &rel, const std::vector<char> &record);
  bool insert_var(Relation &rel, const std::vector<char> &record);

  void insertFromShell(const std::string &relation_name,
                       const std::vector<std::string> &values);
  void insertFromShell_fix(const std::string &relation_name,
                           const std::vector<std::string> &values);
  void insertFromShell_var(const std::string &relation_name,
                           const std::vector<std::string> &values);

  void printRelation(const std::string &relation_name);
  void printRelation_fix(const std::string &relation_name);
  void printRelation_var(const std::string &relation_name);

  void selectWhere(const std::string &relation_name,
                   const std::string &field_name, const std::string &value,
                   const std::string &op,
                   const std::string &output_name = "temp_result");
  void selectWhere_fix(const std::string &relation_name,
                       const std::string &field_name, const std::string &value,
                       const std::string &op,
                       const std::string &output_name = "temp_result");
  void selectWhere_var(const std::string &relation_name,
                       const std::string &field_name, const std::string &value,
                       const std::string &op,
                       const std::string &output_name = "temp_result");

  void createOrReplaceRelationFromCSV_fix(const std::string &relation_name,
                                          const std::string &csv_path);
  void createOrReplaceRelationFromCSV_var(const std::string &relation_name,
                                          const std::string &csv_path);

  void insertNFromCSV(const std::string &relation_name,
                      const std::string &csv_path, int N);
  void insertNFromCSV_fix(const std::string &relation_name,
                          const std::string &csv_path, int N);

  void insertNFromCSV_var(const std::string &relation_name,
                          const std::string &csv_path, int N);

  void deleteWhere(const std::string &relation_name,
                   const std::string &field_name, const std::string &value,
                   const std::string &op);
  void deleteWhere_fix(const std::string &relation_name,
                       const std::string &field_name, const std::string &value,
                       const std::string &op);
  void deleteWhere_var(const std::string &relation_name,
                       const std::string &field_name, const std::string &value,
                       const std::string &op);
  void compactBlock_var(int block_idx);

  void modifyFromShell(const std::string &relation_name,
                       const std::string &field_name, const std::string &value,
                       const std::vector<std::string> &new_values);
  void modifyFromShell_fix(const std::string &relation_name,
                           const std::string &field_name,
                           const std::string &value,
                           const std::vector<std::string> &new_values);

  void modifyFromShell_var(const std::string &relation_name,
                           const std::string &field_name,
                           const std::string &value,
                           const std::vector<std::string> &new_values);

  void printBlock(int block_idx);
  void printRelationSchema(const std::string &relation_name);
  void printDiskCapacityInfo();
};
