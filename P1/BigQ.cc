#include "BigQ.h"
#include "Record.h"
#include "vector"
#include "algorithm"

void *working(void *big)
{
	BigQ *bigQ = (BigQ *)big;

	Record temp;
	Page bufferPage;

	int currentRunSizeInBytes = 0;

	Schema mySchema("catalog", "customer");

	//stores all the runs
	vector<vector<Record *> > runsVector;
	vector<Record *> currentRun;

	//PAGE_SIZE is sum of the twoWayList of records and 2 integer variables
	int runSizeInBytes = (PAGE_SIZE - 2 * sizeof(int)) * bigQ->runLength;

	//Read all records
	while (bigQ->inPipe->Remove(&temp))
	{
		Record *record = new Record();
		record->Consume(&temp);

		//record's first sizeof(int) bytes represent it's size in number of bytes,
		int currentRecordSizeInBytes = ((int *)record->GetBits())[0];

		if (currentRunSizeInBytes + currentRecordSizeInBytes < runSizeInBytes)
		{
			currentRun.push_back(record);
			currentRunSizeInBytes += currentRecordSizeInBytes;
		}
		else
		{
			runsVector.push_back(currentRun);
			currentRun.clear();
			currentRun.push_back(record);
			currentRunSizeInBytes = currentRecordSizeInBytes;
		}
	}
	runsVector.push_back(currentRun);

	//sort all runs seperately
	for (int i = 0; i < runsVector.size(); i++)
	{
		sort(runsVector[i].begin(), runsVector[i].end(), bigQ->comparator);
	}

	//TO PONDER OVER - WHY IT DIDN'T WORK
	// for (vector< Record* > run : runsVector){

	// 	sort(run.begin(), run.end(), bigQ->comparator);

	// }
#ifdef printRecs
	int runNum = 0;
	for (vector<Record *> run : runsVector)
	{
		cout << "run: " << runNum << endl;
		for (Record *s : run)
		{
			s->Print(&mySchema);
			
		}
		runNum++;
		cout << "-------------------------" << endl;
	}
#endif
}

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	// read data from in pipe sort them into runlen pages

	inPipe = &in;
	outPipe = &out;
	comparator.sortOrder = sortorder;
	runLength = runlen;

	pthread_t worker;

	//create worker thread
	int w = pthread_create(&worker, NULL, working, (void *)this);
	if (w)
	{
		printf("Error creating worker thread! Return %d\n", w);
		exit(-1);
	}

	// construct priority queue over sorted runs and dump sorted data
	// into the out pipe

	// finally shut down the out pipe

	//wait for thread to finish, then release thread stack
	pthread_join(worker, NULL);

	//release lock on the out pipe, only when completely done
	out.ShutDown();
}

BigQ::~BigQ()
{
}
