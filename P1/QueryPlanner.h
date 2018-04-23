#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H
#define MAX_NUM_RELS 20
#define MAX_ATTS 100

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
  friend class GroupByNode;

protected:
  Schema *outSchema;
  string operationName;
  char *relationNames[MAX_NUM_RELS]; //populated differently for different things
  int numRelations;
  int outPipeID;
  Statistics *statistics;
  int estimatedTuples;
  // int optimalTuples;
  // Statistics *statistics;

  OperationNode *root;

public:
  //constructors
  OperationNode(string operationName, Statistics *statistics, int outPipeID);
  OperationNode(string operationName, Statistics *statistics, Schema *outSchema, int outPipeID);

  // OperationNode(string operationName, Schema *outSchema, string, Statistics *statistics);
  // OperationNode(string operationName, Schema *outSchema, vector<string>, Statistics *statistics);

  virtual void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const = 0;

  virtual ~OperationNode();
};

class SingletonLeafNode : public OperationNode
{
private:
  char *aliasName;

public:
  SingletonLeafNode(string operationName, Statistics *statistics, Schema *outSchema, int outPipeId, char *relationName, char *aliasName);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

class JoinOperationNode : public OperationNode
{

private:
  OperationNode *leftOperationNode;
  OperationNode *rightOperationNode;

public:
  JoinOperationNode(string operationName, Statistics *Statistics, int outPipeID, OperationNode *node1, OperationNode *node2);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  void combineRelNames();
  void populateOutSchema();

  CNF cnf;
  Record literal;
};

class ProjectOperationNode : public OperationNode
{
  friend class QueryPlanner;
  ProjectOperationNode(NameList *atts, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;

  int keepMe[MAX_ATTS];
  int numInputAtts;
  int numProjectedAtts;
};

class SelectOperationNode : public OperationNode
{
};

class DupRemovalOperationNode : public OperationNode
{
  friend class QueryPlanner;

  DupRemovalOperationNode(OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  OrderMaker dupRemovalOrder;
};

class SumOperationNode : public OperationNode
{
  friend class QueryPlanner;

  SumOperationNode(FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  Schema *resultSchema(FuncOperator *parseTree, OperationNode *node);
  Function func;
};

class GroupByOperationNode : public OperationNode
{
  friend class QueryPlanner;

  GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  Schema *resultSchema(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
  OrderMaker groupOrder;
  Function func;
};

class WriteOperationNode : public OperationNode
{
  friend class QueryPlanner;

  WriteOperationNode(FILE *outFile, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  FILE *outputFile;
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

  void performSum();
  void performProject();

  void print(std::ostream &os = std::cout) const;
};
#endif