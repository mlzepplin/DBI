#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H
#define MAX_NUM_RELS 20
#include "Schema.h"
#include "Statistics.h"
#include "Function.h"
#include "ParseTree.h"
#include "Comparison.h"
#include <iostream>
#include <string>
#include <vector>

extern FuncOperator *finalFunction;
extern TableList *tables;
extern AndList *boolean;
extern NameList *groupingAtts;
extern NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

//QUESTION? - DOES EVERY NODE NEED TO MAINTAIN ITS OWN STATS OBJECT
//OR ALL REFER TO A COMMON OBJECT THAT GETS UPDATED FOR EACH AS WE GO ALONG?

class OperationNode
{
  friend class QueryPlanner;
  friend class JoinOperationNode;

protected:
  Schema *outSchema;
  string operationName;
  char *relationNames[MAX_NUM_RELS]; //populated differently for different things
  int numRelations;
  int outPipeID;
  Statistics *statistics;
  // int estimatedTuples;
  // int optimalTuples;
  // Statistics *statistics;

public:
  //constructors
  OperationNode(string operationName, Statistics *statistics, int outPipeID);
  OperationNode(string operationName, Statistics *statistics, Schema *outSchema, int outPipeID);
  // OperationNode(string operationName, Schema *outSchema, string, Statistics *statistics);
  // OperationNode(string operationName, Schema *outSchema, vector<string>, Statistics *statistics);
};

class SingletonLeafNode : public OperationNode
{
private:
  char *aliasName;

public:
  SingletonLeafNode(string operationName, Statistics *statistics, Schema *outSchema, int outPipeId, char *relationName, char *aliasName);
};

class JoinOperationNode : public OperationNode
{

private:
  OperationNode *leftOperationNode;
  OperationNode *rightOperationNode;

public:
  JoinOperationNode(string operationName, Statistics *statistics, int outPipeID, OperationNode *node1, OperationNode *node2);
  void combineRelNames();
  void populateOutSchema();
};
class ProjectOperationNode : public OperationNode
{
};
class SelectOperationNode : public OperationNode
{
};
class DupRemovalOperationNode : public OperationNode
{
};
class SumOperationNode : public OperationNode
{
};
class GroupByOperationNode : public OperationNode
{
};

class QueryPlanner
{

private:
  OperationNode *root;
  vector<OperationNode *> nodesVector;
  char *outFilePath;
  FILE *outFile;
  char *inFilePath;
  Statistics *statistics;

public:
  QueryPlanner(Statistics *statistics)
  {
    this->statistics = statistics;
  }
  void initLeaves();
  void planJoins();
  void planOperationOrder();
  void printOperationOrder();
};
#endif