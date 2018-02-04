#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <iostream>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    switch(f_type) {
    case heap:
          {
              //create heap file
              exit(0);
          }
      case sorted:
          {
              cout<<"Not implemented yet";
              exit(1);
          }
      case tree:
          {
              cout<<"Not implemented yet";
              exit(1);
          }
      default:
          cerr<<"Invalid file type";
          exit(1);
    }
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
}

int DBFile::Open (const char *f_path) {
}

void DBFile::MoveFirst () {
}

int DBFile::Close () {
}

void DBFile::Add (Record &rec) {
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
