#ifndef RECORD_H
#define RECORD_H

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#include "Defs.h"
#include "ParseTree.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

// Basic record data structure. Data is actually stored in "bits" field. The layout of bits is as follows:
//	1) First sizeof(int) bytes: length of the record in bytes
//	2) Next sizeof(int) bytes: byte offset to the start of the first att
//	3) Byte offset to the start of the att in position numAtts
//	4) Bits encoding the record's data

class Record
{

	friend class ComparisonEngine;
	friend class Page;

  private:
	void SetBits(char *bits);
	void CopyBits(char *bits, int b_len);

  public:
	char *bits;
	Record();
	template <class T>
	Record(const T &value);
	~Record();

	//made public getBits
	char *GetBits();

	// suck the contents of the record fromMe into this; note that after
	// this call, fromMe will no longer have anything inside of it
	void Consume(Record *fromMe);

	// make a copy of the record fromMe; note that this is far more
	// expensive (requiring a bit-by-bit copy) than Consume, which is
	// only a pointer operation
	void Copy(Record *copyMe);

	// reads the next record from a (pointer to a text file); also requires
	// that the schema be given; returns a 0 if there is no data left or
	// if there is an error and returns a 1 otherwise
	int SuckNextRecord(Schema *mySchema, FILE *textFile);

	int ComposeRecord(Schema *mySchema, const char *src);

	// this projects away various attributes...
	// the array attsToKeep should be sorted, and lists all of the attributes
	// that should still be in the record after Project is called.  numAttsNow
	// tells how many attributes are currently in the record
	void Project(int *attsToKeep, int numAttsToKeep, int numAttsNow);

	// takes two input records and creates a new record by concatenating them;
	// this is useful for a join operation
	// attsToKeep[] = {0, 1, 2, 0, 2, 4} --gets 0,1,2 records from left 0, 2, 4 recs from right and startOfRight=3
	// startOfRight is the index position in attsToKeep for the first att from right rec
	void MergeRecords(Record *left, Record *right, int numAttsLeft,
					  int numAttsRight, int *attsToKeep, int numAttsToKeep, int startOfRight);

	// prints the contents of the record; this requires
	// that the schema also be given so that the record can be interpreted
	void Print(Schema *mySchema);

	//Write record to a file
	void Write(FILE *file, Schema *mySchema);

	// Get number of attributes
	int getNumAtts();

	// atomic merge records into me
	void atomicMerge(Record *left, Record *right);

	template <class groupByType>
	void AddSumAsFirstAtribute(const groupByType &value);

	int getLength() { return ((int *)bits)[0]; }
};

template <class T> // T should be int or double
Record::Record(const T &value)
{
	//check type of value and allocate memory for the record
	size_t totalSpace = 2 * sizeof(int) + sizeof(T);
	bits = new char(totalSpace);

	//store length of record in bytes in first sizeof(int) bytes
	((int *)bits)[0] = totalSpace;

	//store offset value
	((int *)bits)[1] = 2 * sizeof(int);

	//store the value after the offset
	*(T *)(bits + ((int *)bits)[1]) = value;
}

template <class groupByType>
void Record::AddSumAsFirstAtribute(const groupByType &value)
{
	size_t attSize = sizeof(int) + sizeof(groupByType);
	size_t recordSize = ((int *)bits)[0] + attSize;

	int numOfAtts = getNumAtts();

	Record groupByRecord;
	if (bits)
		delete[] bits;
	bits = new char[recordSize];
	((int *)bits)[0] = recordSize;
	((int *)bits)[1] = sizeof(int) * (numOfAtts + 2);

	size_t offset = ((int *)bits)[1];
	*(groupByType *)(bits + offset) = value;

	for (int i = 0; i < numOfAtts; i++)
	{
		((int *)bits)[i + 2] = ((int *)bits)[i + 1] + attSize;
	}

	memcpy((void *)(groupByRecord.bits + ((int *)bits)[1] + attSize), (void *)(bits + ((int *)bits)[1]), ((int *)bits)[0] - ((int *)bits)[1]);
	Consume(&groupByRecord);
}

#endif
