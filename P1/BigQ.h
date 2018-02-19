#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ
{

  public:
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker sortOrder;
	int runLength;

	File *tempFile;

	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	//bool Sorter(Record *i, Record *j);
	~BigQ();
};

#endif
