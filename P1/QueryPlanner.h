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
class OperationNode;
class JoinOperationNode;
class GroupByOperationNode;
class SingletonLeafNode;
class WriteOperationNode;
class DupRemovalOperationNode;
class SumOperationNode;
class ProjectOperationNode;

class QueryPlanner
{

private:
  OperationNode *root;
  vector<OperationNode *> nodesVector;         //represents the pseudo tree
  vector<JoinOperationNode *> joinNodesVector; //represents the join pseudo tree
  char *outFilePath;
  FILE *outFile;
  std::ofstream outStream;
  Statistics *statistics;
  AndList *andList;
  static int pipeId;
  std::string outMode;

public:
  QueryPlanner(Statistics *statistics, char *outFilePath, AndList *andList)
  {
    this->statistics = statistics;
    this->outFilePath = outFilePath;
    this->andList = andList;
    outStream.open(outFilePath);
  }

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
  void deepCopyAndList(AndList *&populateMe, AndList *copyMe);

  void print(std::ostream &os = std::cout);
  void setOutputMode(char *out);
};

class OperationNode
{
  friend class QueryPlanner;
  friend class JoinOperationNode;
  friend class GroupByOperationNode;
  friend class SingletonLeafNode;
  friend class WriteOperationNode;
  friend class DupRemovalOperationNode;
  friend class SumOperationNode;
  friend class SelectOperationNode;
  friend class ProjectOperationNode;

protected:
  Schema *outSchema;
  string operationName;

  char *relationNames[MAX_NUM_RELS]; //populated differently for different things
  int numRelations;

  int outPipeId;
  static int pipeId;

  Statistics *statistics;
  int estimatedTuples;
  int numTuples;
  // int optimalTuples;
  // Statistics *statistics;

  OperationNode *root;

public:
  //constructors
  OperationNode(string operationName);
  OperationNode(string operationName, Schema *outSchema);
  OperationNode(string operationName, Statistics *statistics);
  OperationNode(string operationName, Statistics *statistics, Schema *outSchema);
  int getNumTuples() { return numTuples; }
  // OperationNode(string operationName, Schema *outSchema, string, Statistics *statistics);
  // OperationNode(string operationName, Schema *outSchema, vector<string>, Statistics *statistics);

  virtual void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) = 0;
  std::string getOperationName();

  //virtual ~OperationNode();
};

class AndListBasedOperationNode : public OperationNode
{
protected:
  CNF cnf;
  Record literal;

public:
  AndListBasedOperationNode(string operationName);
  AndList *buildSubAndList(AndList *&boolean);
  bool isValidOr(OrList *orList);
  virtual bool isValidComparisonOp(ComparisonOp *compOp) = 0;
};

class JoinOperationNode : public AndListBasedOperationNode
{
  friend class QueryPlanner;

private:
  OperationNode *leftOperationNode;
  OperationNode *rightOperationNode;

public:
  JoinOperationNode(OperationNode *node1, OperationNode *node2);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0);
  void combineRelNames();
  void populateJoinOutSchema();
  bool isValidComparisonOp(ComparisonOp *compOp);
};

class SelectOperationNode : public AndListBasedOperationNode
{
private:
  char *aliasName;

public:
  SelectOperationNode(Statistics *&statistics, Schema *outSchema, char *relationName, char *aliasName);
  bool isValidComparisonOp(ComparisonOp *compOp);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0);
};

class ProjectOperationNode : public OperationNode
{

  int keepMe[MAX_ATTS];
  int numInputAtts;
  int numProjectedAtts;

public:
  ProjectOperationNode(NameList *atts, OperationNode *node); //will update outschema from inside
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0);
};

class DupRemovalOperationNode : public OperationNode
{
  OrderMaker dupRemovalOrder;

public:
  DupRemovalOperationNode(OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0);
};

class SumOperationNode : public OperationNode
{
  Function func;

public:
  SumOperationNode(FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0);
  Schema *resultSchema(FuncOperator *parseTree, OperationNode *node);
};

class GroupByOperationNode : public OperationNode
{

  OrderMaker groupOrder;
  Function func;

public:
  GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0);
  Schema *resultantSchema(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
};

class WriteOperationNode : public OperationNode
{
  friend class QueryPlanner;

public:
  WriteOperationNode(FILE *&outFile, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0);
  FILE *outputFile;
};

#endif