#include "BigQ.h"
#include "Record.h"
#include "vector"
#include "algorithm"

void *working(void *big)
{
	BigQ *bigQ = (BigQ *)big;

	Record temp;
	Page bufferPage;

	int numPages = 0;

	Schema mySchema("catalog", "region");

	//stores all the runs
	vector<vector<Record *> > runsVector;
	vector<Record *> currentRun;
	Record *r;

	//Read all records
	while (bigQ->inPipe->Remove(&temp))
	{
		///currentRun.push_back(&temp);
		//cout<<&temp<<"temp"<<endl;
		//EACH TIME THE R WILL BE ALLOCATED MEM AT A DIFFERENT
		//LOCATION AND WILL CONSUME TEMP
		r = new Record();
		r->Consume(&temp);
		currentRun.push_back(r);

		//bigQ->outPipe->Insert(&temp);

		// if (numPages < bigQ->runLength)
		// {

		// 	Record *record = new Record();
		// 	record->Consume(&temp);
		// 	currentRun.push_back(record);

		// 	//Append returns 0 if record can't fit in page
		// 	if (!bufferPage.Append(&temp))
		// 	{
		// 		numPages++;
		// 		bufferPage.EmptyItOut();
		// 		bufferPage.Append(&temp);
		// 	}
		// }
		// else
		// {
		// 	runsVector.push_back(currentRun);
		// 	currentRun.clear();
		// 	numPages = 0;
		// }
	}

	//sort run
	sort(currentRun.begin(), currentRun.end(), bigQ->comparater);
	//cout<<"cur run size: "<<currentRun.size()<<endl;
	// for (int i = 0; i < currentRun.size(); i++)
	// {	cout<<"skj"<<endl;
	// 	//cout<<currentRun[i]<<endl;
	// 	currentRun[i]->Print(&mySchema);
	// }
	for (Record *s : currentRun)
        s->Print(&mySchema);

}

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	// read data from in pipe sort them into runlen pages

	inPipe = &in;
	outPipe = &out;
	sortOrder = sortorder;
	comparater.sortOrder = this->sortOrder;
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
	pthread_join (worker, NULL);

	//release lock on the out pipe, only when completely done
	out.ShutDown();
}

BigQ::~BigQ()
{
}
