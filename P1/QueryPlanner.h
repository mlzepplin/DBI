#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H
#define MAX_NUM_RELS 20
#define MAX_ATTS 100

#include "Schema.h"
#include "Statistics.h"
#include "Function.h"
#include "ParseTree.h"
#include "Comparison.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>

extern FuncOperator *finalFunction;
extern TableList *tables;
extern AndList *boolean;
extern NameList *groupingAtts;
extern NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

//QUESTION? - DOES EVERY NODE NEED TO MAINTAIN ITS OWN STATS OBJECT
//OR ALL REFER TO A COMMON OBJECT THAT GETS UPDATED FOR EACH AS WE GO ALONG?
class QueryPlanner;
class JoinOperationNode;
class GroupByNode;
class SingletonLeafNode;
class WriteOperationNode;
class DupRemovalOperationNode;
class SumOperationNode;
class ProjectOperationNode;

class OperationNode
{
  friend class QueryPlanner;
  friend class JoinOperationNode;
  friend class GroupByNode;
  friend class SingletonLeafNode;
  friend class WriteOperationNode;
  friend class DupRemovalOperationNode;
  friend class SumOperationNode;

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
  OperationNode(string operationName, int outPipeID);
  OperationNode(string operationName, Schema *outSchema, int outPipeID);
  OperationNode(string operationName, Statistics *statistics, int outPipeID);
  OperationNode(string operationName, Statistics *statistics, Schema *outSchema, int outPipeID);

  // OperationNode(string operationName, Schema *outSchema, string, Statistics *statistics);
  // OperationNode(string operationName, Schema *outSchema, vector<string>, Statistics *statistics);

  virtual void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const = 0;
  std::string getOperationName();
  AndList *buildSubAndList(AndList *boolean, Schema *schema);
  bool isValidCondition(OrList *orList, Schema *joinSchema);
  bool isValidCondition(ComparisonOp *compOp, Schema *joinSchema);

  //virtual ~OperationNode();
};

class SingletonLeafNode : public OperationNode
{
  // friend class QueryPlanner;

private:
  char *aliasName;

public:
  SingletonLeafNode(Statistics *statistics, Schema *outSchema, int outPipeId, char *relationName, char *aliasName);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

class JoinOperationNode : public OperationNode
{
  // friend class QueryPlanner;

private:
  OperationNode *leftOperationNode;
  OperationNode *rightOperationNode;

public:
  JoinOperationNode(Statistics *Statistics, int outPipeID, OperationNode *node1, OperationNode *node2);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  void combineRelNames();
  void populateJoinOutSchema();

  CNF cnf;
  Record literal;
};

class ProjectOperationNode : public OperationNode
{
  // friend class QueryPlanner;

  int keepMe[MAX_ATTS];
  int numInputAtts;
  int numProjectedAtts;

public:
  ProjectOperationNode(NameList *atts, OperationNode *node, int pipeId); //will update outschema from inside
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

class DupRemovalOperationNode : public OperationNode
{
  //friend class QueryPlanner;
  OrderMaker dupRemovalOrder;

public:
  DupRemovalOperationNode(OperationNode *node, int pipeId);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

class SumOperationNode : public OperationNode
{
  // friend class QueryPlanner;
  Function func;

public:
  SumOperationNode(FuncOperator *parseTree, OperationNode *node, int pipeId);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  Schema *resultSchema(FuncOperator *parseTree, OperationNode *node);
};

class GroupByOperationNode : public OperationNode
{
  // friend class QueryPlanner;

  OrderMaker groupOrder;
  Function func;

public:
  GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  Schema *resultantSchema(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
};

class WriteOperationNode : public OperationNode
{
  // friend class QueryPlanner;

  FILE *outputFile;

public:
  WriteOperationNode(FILE *outFile, OperationNode *node, int pipeId);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

class QueryPlanner
{

private:
  OperationNode *root;
  vector<OperationNode *> nodesVector;
  char *outFilePath;
  FILE *outFile;
  std::ofstream outStream;
  char *inFilePath;
  Statistics *statistics;
  AndList *andList;

public:
  QueryPlanner(Statistics *statistics, char *inFilePath, char *outFilePath, AndList *andList)
  {
    this->statistics = statistics;
    this->inFilePath = inFilePath;
    this->outFilePath = outFilePath;
    this->andList = andList;
    outStream.open(outFilePath);
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