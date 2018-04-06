#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <list>
#include <string>
#include "Schema.h"
struct RelationInfo
{
	int numTuples;
	std::unordered_map<std::string, int> attributeMap;
} typedef RelationInfo;

class Statistics
{
  private:
	std::unordered_map<std::string, RelationInfo> relationMap;
	std::list<unordered_set<string>> joinList;

  public:
	Statistics();
	Statistics(Statistics &copyMe); // Performs deep copy
	~Statistics();
	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif
