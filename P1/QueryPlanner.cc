#include "QueryPlanner.h"
#include "float.h"

extern char *catalogPath;

void QueryPlanner::planOperationOrder()
{
    initLeaves();
    planJoins();
}

void QueryPlanner::printOperationOrder()
{
}
void QueryPlanner::planJoins()
{
    //steps to do
    //the singletons should already be populated as leaf representation of the (pseudo)tree
    //first we'll call for join's optimal estimate
    //second, to it, we'll append the other estimates, which are unary in nature
    //and then what we print it out using the orint helper methid to the outFile
    vector<OperationNode *> optimalNodesVector;
    vector<OperationNode *>::iterator nodesVecIter;
    double optimalEstimate = DBL_MAX;
    //get all the permutations of the list and break at the optimal one
    //RMEEMBER - DON'T ALTER THE ACTUAL LEAF LIST!!

    /*
Schema *outSchema;
  string operationName;
  char *relationNames[MAX_NUM_RELS];
  int estimatedTuples;
  int optimalTuples;
  Statistics *statistics;
  int outPipeID;
*/
    while (std::next_permutation(nodesVector.begin(), nodesVector.end()))
    {
        vector<OperationNode *> currentNodesVector = nodesVector;
        Statistics stats = *statistics;
        double currentEstimate = 0.0;
        while (currentNodesVector.size() > 1)
        {

            //pop off the last two
            OperationNode *node1 = currentNodesVector.back();
            currentNodesVector.pop_back();
            OperationNode *node2 = currentNodesVector.back();
            currentNodesVector.pop_back();

            //constructing the outSchema for the join

            //constructing new JoinNode which inherently updates relNames,numRelations as well
            int outPipeId = std::max(node1->outPipeID, node2->outPipeID) + 1;
            OperationNode *joinNode = new JoinOperationNode("join", statistics, outPipeId, node1, node2);

            //join em and
            currentEstimate += stats.Estimate(boolean, joinNode->relationNames, joinNode->numRelations);
            stats.Apply(boolean, joinNode->relationNames, joinNode->numRelations);

            currentNodesVector.push_back(joinNode);
        }
        if (currentEstimate < optimalEstimate)
        {
            optimalEstimate = currentEstimate;
            optimalNodesVector = currentNodesVector;
        }
        //for this permutation, calc the total cost
        //estimatedTuples += statistics->Estimate();
    }
    //note, at the end when all the joins are done, we'll have only one
    //node (JoinOperationNode) in the optimalNodesVector, and its outschema
    //will be all the prepended alisaed attributes
    ((JoinOperationNode *)optimalNodesVector[0])->populateOutSchema(stats);
}
void JoinOperationNode::populateOutSchema(Statistics *stats)
{
    //go through all the table names, get their alias from aliasmappings
    //find their attributes from the relation map and build outschema

    while (tables != NULL)
    {
        stats->RelationMap(tables->tableName;
        cout << tables->aliasAs;
        tables = tables->next;
        cout << endl;
    }
}
JoinOperationNode::JoinOperationNode(string operationName, Statistics *Statistics, int outPipeID, OperationNode *node1, OperationNode *node2) : OperationNode(operationName, statistics, outPipeID)
{
    leftOperationNode = node1;
    rightOperationNode = node2;
    combineRelNames();
}
void JoinOperationNode::combineRelNames()
{
    int numLeft = leftOperationNode->numRelations;
    int numRight = rightOperationNode->numRelations;
    //update numRelations of new joinnode
    numRelations = numLeft + numRight;
    //update relNames[] for new join node
    for (int i = 0; i < numLeft; i++)
    {
        relationNames[i] = leftOperationNode->relationNames[i];
    }
    for (int i = numLeft; i < numRelations; i++)
    {
        relationNames[i] = rightOperationNode->relationNames[i];
    }
}
void QueryPlanner::initLeaves()
{
    // statistics->Read(inFilePath);
    while (tables != NULL)
    {
        statistics->CopyRel(tables->tableName, tables->aliasAs);
        aliasMappings.insert(std::make_pair(tables->tableName, tables->aliasAs));
        Schema *outSchema = new Schema(catalogPath, tables->tableName);
        int outPipeId = 1;
        OperationNode *currentNode = new SingletonLeafNode("leaf", outSchema, outPipeId, tables->tableName, tables->aliasAs);
        nodesVector.push_back(currentNode);
        tables = tables->next;
    }
}
