#ifndef DB_H
#define DB_H

#include "Record.h"
#include "Schema.h"
#include "fTypeEnum.h"
#include <string>
#include <fstream>
using namespace std;

class DB
{
  protected:
    FILE *database;
    FILE *tblFile;
    Page bufferPage;
    Record *currentRecord;
    File dFile;
    off_t currentPageOffset;

    string tableName;
    string binPath; 
    ofstream auxFile;


public:
  DB() { currentPageOffset = 0; }
  virtual ~DB(){};

  virtual int Open(const char *fpath) = 0;
  virtual void MoveFirst() = 0;
  virtual int GetNext(Record &fetchme) = 0;
  virtual int Create(const char *fpath, void *startup) = 0;
  virtual int Close() = 0;
  virtual void Add(Record &addme) = 0;
  virtual void Load(Schema &myschema, const char *loadpath) = 0;
  virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
  
  static string getTableName(const char *fpath)
  {
    string filePath(fpath);
    size_t st = filePath.find_last_of('/'), end = filePath.find_last_of('.');
    return filePath.substr(st + 1, end - st - 1);
  }
  static string getBinPath(const char *fpath){
    string filePath(fpath);
    size_t end = filePath.find_last_of('/');
    return filePath.substr(0,end);
  }
  void moveFirstWithoutModeSwitch(){
    currentPageOffset = 0;
    bufferPage.EmptyItOut();
    dFile.GetPage(&bufferPage, 0);
    currentPageOffset++;
  }
};
#endif