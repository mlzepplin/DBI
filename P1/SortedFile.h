#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DB.h"
#include "BigQ.h"

enum FileMode
{
	read,
	write
};

class SortedFile : public DB
{
  private:
	BigQ *bigQ;
	Pipe *inPipe;
	Pipe *outPipe;
	FileMode mode;
	int runLength;
	OrderMaker sortOrder;

  public:
	SortedFile();
	~SortedFile();

	int Create(const char *fpath, void *startup);
	int Open(const char *fpath);
	int Close();

	void Load(Schema &myschema, const char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

	void startReading();
	void startWriting();
	void createBigQ();
	void mergeBiQAndDfile();
	void deleteBigQ();
	int binarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& comp);
};

#endif
