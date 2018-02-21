#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

struct Comp{

	OrderMaker sortOrder;

	bool operator()(Record *a, Record *b){
		
		ComparisonEngine compEngine;
		cout<<"sorder: "<<compEngine.Compare(a, b, &sortOrder)<<endl;
		if(compEngine.Compare(a, b, &sortOrder) < 0)
			return true;
		else 
			return false;
	}
};
class BigQ
{

  public:
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker sortOrder;
	Comp comparater;
	int runLength;

	File *tempFile;

	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	//bool Sorter(Record *i, Record *j);
	~BigQ();
};

#endif
