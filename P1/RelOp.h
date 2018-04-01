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
	bool addRecord(Record &rec)
	{
		if (buffer.size() + 1 > maxSize)
			return false;
		buffer.push_back(&rec);
		return true;
	}
	Record *getRecord(int index)
	{
		return buffer[index];
	}
	int size()
	{
		return buffer.size();
	}
	void clear()
	{
		buffer.clear();
	}
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
	JoinMemBuffer joinMemBuffer;

  public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
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
	template <class sumType>
	static Record groupBySum(Record *a, Record *b, Function *computeMe);
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
	outPipe->Insert(&sumRecord);
}

template <class sumType>
Record GroupBy::groupBySum(Record *a, Record *b, Function *computeMe)
{
	sumType sum = 0;
	Record buffer;

	sum += computeMe->Apply<sumType>(a);
	sum += computeMe->Apply<sumType>(b);
	//create an instance of a Record that contains the calculated sum of the given function
	Record sumRecord(sum);
	return sumRecord;
}

#endif
