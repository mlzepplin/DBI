#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H

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
private:
  Schema *outSchema;
  string operationName;
  vector<string> relationNames;
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
class UnaryOperationNode : protected OperationNode
{
};
class BinaryOperationNode : protected OperationNode
{
};
class SingletonLeafNode : protected OperationNode
{
public:
  SingletonLeafNode(string operationName, Schema *outSchema, Statistics *statistics) : OperationNode(operationName, outSchema, statistics)
  {
  }
};
class JoinOperationNode : protected BinaryOperationNode
{
};
class ProjectOperationNode : protected UnaryOperationNode
{
};
class SelectOperationNode : protected UnaryOperationNode
{
};
class DupRemovalOperationNode : protected UnaryOperationNode
{
};
class SumOperationNode : protected UnaryOperationNode
{
};
class GroupByOperationNode : protected UnaryOperationNode
{
};

class QueryPlanner
{
private:
  OperationNode *root;
  vector<OperationNode *> nodesVector;
  string outFileName;
  FILE *outFile;
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
  void planOperationOrder();
  void printOperationOrder();
};
#endif