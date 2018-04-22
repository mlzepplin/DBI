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
//QUESTION? - DOES EVERY NODE NEED TO MAINTAIN ITS OWN STATS OBJECT
//OR ALL REFER TO A COMMON OBJECT THAT GETS UPDATED FOR EACH AS WE GO ALONG?
class OperationNode
{
protected:
  Schema *outSchema;
  string operationName;
  char *relationNames[MAX_NUM_RELS];
  int numRelations;
  int estimatedTuples;
  int optimalTuples;
  Statistics *statistics;
  int outPipeID;

public:
  //constructors
  OperationNode(string operationName, Schema *outSchema, Statistics *statistics);
  OperationNode(string operationName, Schema *outSchema, string, Statistics *statistics);
  OperationNode(string operationName, Schema *outSchema, vector<string>, Statistics *statistics);
};

class SingletonLeafNode : public OperationNode
{
  friend class JoinOperationNode;

private:
  char *aliasName;

public:
  SingletonLeafNode(char *relationName, char *aliasName, Schema *outSchema, Statistics *statistics) : OperationNode(operationName, outSchema, statistics)
  {
    this->relationNames[0] = relationName;
    this->aliasName = aliasName;
    this->outSchema = outSchema;
    this->statistics = statistics;
    numRelations = 1;
  }
};
class JoinOperationNode : public OperationNode
{

public:
  OperationNode *leftOperationNode;
  OperationNode *rightOperationNode;
  JoinOperationNode(OperationNode *left, OperationNode *right);
  void combineRelNames();
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
  unordered_map<char *, char *> aliasMappings;
  char *outFilePath;
  FILE *outFile;
  char *inFilePath;
  Statistics *statistics;

  FuncOperator *finalFunction;
  TableList *tables;
  AndList *boolean;
  NameList *groupingAtts;
  NameList *attsToSelect;
  int distinctAtts;
  int distinctFunc;

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