#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

//for sorting runs
class Comp
{
	public:
	OrderMaker sortOrder;
	bool operator()(Record * a, Record * b)
	{
		ComparisonEngine compEngine;

		if (compEngine.Compare(a, b, &sortOrder) > 0)
			return true;
		else
			return false;
	}
};

//for priority_queue
class CompPair
{	
	public:
	OrderMaker sortOrder;
	CompPair(OrderMaker s){
		this->sortOrder = s;
	}
	CompPair(){}
	bool operator()(pair<int,Record *> a, pair<int,Record *> b)
	{
		ComparisonEngine compEngine;

		if (compEngine.Compare(a.second, b.second, &sortOrder) > 0)
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
	Comp comparator;
	int runLength;
	OrderMaker sortOrder;
	pthread_t worker;

	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	void WaitUntilDone();
	bool compare(Record *i, Record *j);
	~BigQ();

};

#endif
