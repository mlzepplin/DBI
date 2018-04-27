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
class GroupByOperationNode;
class SingletonLeafNode;
class WriteOperationNode;
class DupRemovalOperationNode;
class SumOperationNode;
class ProjectOperationNode;

class OperationNode
{
  friend class QueryPlanner;
  friend class JoinOperationNode;
  friend class GroupByOperationNode;
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
  static int pipeId;

  Statistics *statistics;
  int estimatedTuples;
  // int optimalTuples;
  // Statistics *statistics;

  OperationNode *root;

public:
  //constructors
  OperationNode(string operationName);
  OperationNode(string operationName, Schema *outSchema);
  OperationNode(string operationName, Statistics *statistics);
  OperationNode(string operationName, Statistics *statistics, Schema *outSchema);

  // OperationNode(string operationName, Schema *outSchema, string, Statistics *statistics);
  // OperationNode(string operationName, Schema *outSchema, vector<string>, Statistics *statistics);

  virtual void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const = 0;
  std::string getOperationName();

  //virtual ~OperationNode();
};

class SingletonLeafNode : public OperationNode
{

private:
  char *aliasName;

public:
  SingletonLeafNode(Statistics *statistics, Schema *outSchema, char *relationName, char *aliasName);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

class AndListBasedOperationNode : public OperationNode
{
public:
  AndListBasedOperationNode(string operationName);
  AndList *buildSubAndList(AndList *boolean, Schema *schema);
  bool buildSubOrList(OrList *orList, Schema *schema);
  virtual bool isValidCondition(ComparisonOp *compOp, Schema *schema) = 0;
  virtual void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const = 0;
};

class JoinOperationNode : public AndListBasedOperationNode
{

private:
  OperationNode *leftOperationNode;
  OperationNode *rightOperationNode;

public:
  JoinOperationNode(Statistics *Statistics, OperationNode *node1, OperationNode *node2);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  void combineRelNames();
  void populateJoinOutSchema();
  bool isValidCondition(ComparisonOp *compOp, Schema *schema);

  CNF cnf;
  Record literal;
};

class SelectOperationNode : public AndListBasedOperationNode
{
public:
  SelectOperationNode(Statistics *statistics);
  bool isValidCondition(ComparisonOp *compOp, Schema *schema);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};
class SelectAfterJoinOperationNode : public AndListBasedOperationNode
{
public:
  SelectAfterJoinOperationNode(Statistics *statistics);
  bool isValidCondition(ComparisonOp *compOp, Schema *schema);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};
class ProjectOperationNode : public OperationNode
{

  int keepMe[MAX_ATTS];
  int numInputAtts;
  int numProjectedAtts;

public:
  ProjectOperationNode(NameList *atts, OperationNode *node); //will update outschema from inside
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

class DupRemovalOperationNode : public OperationNode
{
  OrderMaker dupRemovalOrder;

public:
  DupRemovalOperationNode(OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

class SumOperationNode : public OperationNode
{
  Function func;

public:
  SumOperationNode(FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  Schema *resultSchema(FuncOperator *parseTree, OperationNode *node);
};

class GroupByOperationNode : public OperationNode
{

  OrderMaker groupOrder;
  Function func;

public:
  GroupByOperationNode(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
  Schema *resultantSchema(NameList *groupingAtts, FuncOperator *parseTree, OperationNode *node);
};

class WriteOperationNode : public OperationNode
{

  FILE *outputFile;

public:
  WriteOperationNode(FILE *outFile, OperationNode *node);
  void printNodeInfo(std::ostream &os = std::cout, size_t level = 0) const;
};

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

public:
  QueryPlanner(Statistics *statistics, char *outFilePath, AndList *andList)
  {
    this->statistics = statistics;
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