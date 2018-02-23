#include "BigQ.h"
#include "Record.h"
#include "vector"
#include "list"
#include "queue"
#include "algorithm"
using namespace std;

void *working(void *big)
{
	BigQ *bigQ = (BigQ *)big;

	Record temp;
	
	int currentRunSizeInBytes = 0;
	int totalRecords = 0;

	//stores all the runs
	vector<vector<Record *> > runsVector;
	vector<Record *> currentRun;

	//PAGE_SIZE is sum of the twoWayList of records and 2 integer variables
	int runSizeInBytes = (PAGE_SIZE - 2 * sizeof(int)) * bigQ->runLength;

    // read data from in pipe sort them into runlen pages
	while (bigQ->inPipe->Remove(&temp))
	{
		totalRecords++;
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

	// construct priority queue over sorted runs and dump sorted data
	// into the out pipe

	//pair holds a record and the runNumber where that record came from
	//so that we can keep track of which run's head to use for the new insert into pq
	priority_queue<pair<int, Record *>, vector<pair<int, Record *> >, CompPair> pq(CompPair(bigQ->sortOrder));

	//init pq with the first element from each run
	for (int i = 0; i < runsVector.size(); i++)
	{
		pq.push(std::make_pair(i, runsVector[i].back()));
		runsVector[i].pop_back();
	}

	for (int i = 0; i < totalRecords; i++)
	{

		//fill outpipe with smallest rec from pq
		bigQ->outPipe->Insert(pq.top().second);

		int runWithSmallestRec = pq.top().first;
		pq.pop();

		if (runsVector[runWithSmallestRec].empty())
		{
			continue;
		}
		else
		{
			pq.push(std::make_pair(runWithSmallestRec, runsVector[runWithSmallestRec].back()));
			runsVector[runWithSmallestRec].pop_back();
		}
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
		cout << "----------------------------------------------------------------" << endl;
	}
#endif
}

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	

	inPipe = &in;
	outPipe = &out;
	sortOrder = sortorder;
	comparator.sortOrder = sortOrder;
	runLength = runlen;

	pthread_t worker;

	//create worker thread
	int w = pthread_create(&worker, NULL, working, (void *)this);
	if (w)
	{
		printf("Error creating worker thread! Return %d\n", w);
		exit(-1);
	}

	//wait for thread to finish, then release thread stack
	pthread_join(worker, NULL);

	// finally shut down the out pipe
	//release lock on the out pipe, only when completely done
	out.ShutDown();
}

BigQ::~BigQ()
{
}
