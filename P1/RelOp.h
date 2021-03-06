#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "BigQ.h"
#include "vector"

class RelationalOp
{
public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone() = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages(int n) = 0;
};

class SelectFile : public RelationalOp
{

private:
	pthread_t thread;
	int numPages;
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	Record *buffer;

public:
	SelectFile();
	void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	static void *selectFileStaticHelper(void *selectFile);
	void *selectFileHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class SelectPipe : public RelationalOp
{

private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	Record *buffer;
	int numPages;

public:
	SelectPipe();
	void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	static void *selectPipeStaticHelper(void *selectPipe);
	void *selectPipeHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Project : public RelationalOp
{
private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	int numPages;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	static void *projectStaticHelper(void *project);
	void *projectHelper();
};
class JoinMemBuffer
{
private:
	int maxSize;
	vector<Record *> buffer;

public:
	JoinMemBuffer(int runLength)
	{
		maxSize = PAGE_SIZE * runLength / sizeof(Record *);
	}
	~JoinMemBuffer() {}
	bool addRecord(Record &rec);
	Record *getRecord(int index);
	int size();
	void clear();
};
class Join : public RelationalOp
{
private:
	pthread_t thread;
	int numPages;
	Pipe *leftInPipe;
	Pipe *rightInPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	Schema *lSchema;
	Schema *rSchema;

	JoinMemBuffer *joinMemBuffer;

public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal, Schema &lSchema, Schema &rSchema);
	static void *joinStaticHelper(void *join);
	void *joinHelper();
	void sortMergeJoin(OrderMaker *leftOrder, OrderMaker *rightOrder);
	void blockNestedLoopJoin(OrderMaker *leftOrder, OrderMaker *rightOrder);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class DuplicateRemoval : public RelationalOp
{
private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *schema;
	int numPages;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	static void *duplicateRemovalStaticHelper(void *duplicateRemoval);
	void *duplicateRemovalHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Sum : public RelationalOp
{

private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;
	int numPages;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	static void *sumStaticHelper(void *sum);
	void *sumHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);

	template <class sumType>
	static void calculateSum(Pipe *inPipe, Pipe *outPipe, Function *computeMe);
};

class GroupBy : public RelationalOp
{
private:
	pthread_t thread;
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;
	OrderMaker *groupAtts;
	int numPages;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	static void *groupbyStaticHelper(void *sum);
	void *groupbyHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);

	template <class groupByType>
	static void performGroupBy(Pipe *inPipe, Pipe *outPipe, OrderMaker *groupAtts, Function *computeMe, size_t numPages);
};

class WriteOut : public RelationalOp
{
private:
	pthread_t thread;
	Pipe *inPipe;
	FILE *outputFile;
	Schema *schema;
	int numPages;

public:
	void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
	static void *writeOutStaticHelper(void *writeOut);
	void *writeOutHelper();
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

template <class sumType>
void Sum::calculateSum(Pipe *inPipe, Pipe *outPipe, Function *computeMe)
{
	sumType sum = 0;
	Record buffer;

	while (inPipe->Remove(&buffer))
	{
		sum += computeMe->Apply<sumType>(buffer);
	}
	//create an instance of a Record that contains the calculated sum of the given function
	Record sumRecord(sum);
	cout << endl;
	cout << "SUMSUMSUMSUMSUSM" << endl;
	outPipe->Insert(&sumRecord);
}

template <class groupByType>
void GroupBy::performGroupBy(Pipe *inPipe, Pipe *outPipe, OrderMaker *groupAtts, Function *computeMe, size_t numPages)
{
	Pipe sortedPipe(PIPE_SIZE);

	//call BigQ to put the sorted output into an intermeddiate sortedPipe
	BigQ bigQ(*inPipe, sortedPipe, *groupAtts, numPages);

	Record buffer, next;
	ComparisonEngine compEngine;

	if (sortedPipe.Remove(&buffer))
	{
		groupByType sum = computeMe->Apply<groupByType>(buffer);

		while (sortedPipe.Remove(&next))
		{
			if (compEngine.Compare(&buffer, &next, groupAtts))
			{
				buffer.Project(groupAtts->whichAtts, groupAtts->numAtts, buffer.getNumAtts());
				buffer.AddSumAsFirstAtribute(sum);
				outPipe->Insert(&buffer);
				buffer.Consume(&next);
				sum = computeMe->Apply<groupByType>(buffer);
			}
			else
			{
				sum += computeMe->Apply<groupByType>(next);
			}
		}
		buffer.Project(groupAtts->whichAtts, groupAtts->numAtts, buffer.getNumAtts());
		buffer.AddSumAsFirstAtribute(sum);
		outPipe->Insert(&buffer);
	}
}
#endif
