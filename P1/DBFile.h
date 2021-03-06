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

using namespace std;
// stub DBFile header..replace it with your own DBFile.h

class DBFile
{
private:
	unordered_map<fType, string> auxMap;
	DB *db;

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



	std::string getTableName(const char* fpath);
	void allocateMemToDB(fType f_type);

};
#endif
