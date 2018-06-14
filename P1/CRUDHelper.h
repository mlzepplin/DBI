#ifndef CRUD_HELPER_H
#define CRUD_HELPER_H

#include <algorithm>
#include <string>

class CRUDHelper
{

public:
  bool createTable();
  bool insertInto();
  bool dropTable();
  void setOutput();

private:
  bool isRelPresent(const char *relname);
  string trim(const string &str);
};

#endif