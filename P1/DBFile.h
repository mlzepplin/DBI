#ifndef DBFILE_H
#define DBFILE_H

#include "DB.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "fTypeEnum.h"
#include <iostream>
#include <unordered_map>

// stub DBFile header..replace it with your own DBFile.h

class DBFile
{
  private:
	string auxFilePath;
	unordered_map<fType, int> auxMap;
	//pthread_rwlock_t rwlock;

  public:
	DBFile();
	~DBFile();

	int Create(const char *fpath, fType file_type, void *startup);
	int Open(const char *fpath);
	int Close();

	void Load(Schema &myschema, const char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

	//Added new methods
	int initReadMode();

	void allocateMemToDB(fType f_type);

  private:
	DB *db;
};
#endif
