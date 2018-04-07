#include "Statistics.h"

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{ //copy constructor, creating a deep copy
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    if (relationMap.find(relName) == relationMap.end())
    {
        //create empty attribute map
        unordered_map<string, int> tempAttributeMap;

        //insert into relation Map
        RelationInfo relationInfo = {numTuples, tempAttributeMap};
        relationMap.insert(std::make_pair(relName, relationInfo));

        //populating join list with singleton
        unordered_set<string> tempRel;
        tempRel.insert(relName);
        joinList.push_front(tempRel);
    }
    else
    { //update numTuples
        relationMap.find(relName)->second.numTuples = numTuples;
    }
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    //need to find the relation first
    if (relationMap.find(relName) == relationMap.end())
    {
        cerr << "the relation to which you're trying to add attribute, does not exist!!" << endl;
        exit(1);
    }
    else
    {
        RelationInfo *relationInfo = &relationMap.find(relName)->second;
        //check if attribute already exists
        if (relationInfo->attributeMap.find(attName) == relationInfo->attributeMap.end())
        { //if not, then add the attribute
            relationInfo->attributeMap.insert(std::make_pair(attName, numDistincts));
        }
        else
        { //update numDistincts
            relationInfo->attributeMap.find(attName)->second = numDistincts;
        }
    }
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    //just provides a copy of the relationInfo object
    RelationInfo relationInfo = relationMap.find(oldName)->second;

    if (relationMap.find(newName) != relationMap.end())
        relationMap.insert(std::make_pair(newName, relationInfo));
    else
    {
        cerr << "relation wih newName already exists" << endl;
        exit(1);
    }
}

void Statistics::Read(char *fromWhere)
{
}
void Statistics::Write(char *fromWhere)
{
}
//TODO
//HELPER METHOD , TO BE REMOVED IF ENDS UP NOT BEING USED
string buildSubsetKey(char *relNames[], int num)
{ //if we take this approach then we'll have to generate all the possible keys
    //and then check for membership
    string key = "";
    std::sort(relNames, relNames + num);
    for (int i = 0; i < num; i++)
    {
        key += relNames[i];
        key += ",";
    }
    return key;
}
bool Statistics::checkAttributes(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr;
    struct ComparisonOp *currentComparisonOp;
    struct Operand *leftOperand, *rightOperand;
    string attName;
    if (parseTree == NULL)
        return true;
    while (currentAnd->rightAnd != NULL)
    {
        currentOr = currentAnd->left;
        while (currentOr->rightOr != NULL)
        {
            currentComparisonOp = currentOr->left;
            leftOperand = currentComparisonOp->left;
            rightOperand = currentComparisonOp->right;
            if (leftOperand->code != NAME)
                return false;
            else
            {
                attName = leftOperand->value;
                if (!findAttInRelation(attName, relNames, numToJoin))
                    return false;
                if (rightOperand->code == NAME)
                {
                    attName = rightOperand->value;
                    if (!findAttInRelation(attName, relNames, numToJoin))
                        return false;
                }
            }
        }
    }
    return true;
}

bool Statistics::findAttInRelation(string attName, char *relNames[], int numToJoin)
{
    unordered_map<string, RelationInfo>::iterator relMapIter;
    unordered_map<string, int>::iterator attMapIter;
    unordered_map<string, int> attMap;

    for (int i = 0; i < numToJoin; i++)
    {
        relMapIter = relationMap.find(relNames[i]);
        if (relMapIter == relationMap.end())
            return false;
        else
        {
            attMap = relMapIter->second.attributeMap;
            attMapIter = attMap.find(attName);
            if (attMapIter == attMap.end())
                return false;
            else
            {
                return true;
            }
        }
    }
    return false;
}
void Statistics::validateJoin(struct AndList *parseTree, char *relNames[], int numToJoin, bool fromApply)
{ //assuming joinList is populated
    unordered_set<string>::iterator subsetIterator, relNamesSetIterator;
    list<unordered_set<string>>::iterator joinListItreator;
    unordered_set<string> relNamesSet, subset;
    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr = currentAnd->left;

    if (!checkAttributes(parseTree, relNames, numToJoin))
    {
        cerr << "either attribute not present in relNames or relNames is not a subset of relationMap" << endl;
        exit(1);
    }
    //making a set version of relNames, to reduce internal lookups to O(1)
    for (int i = 0; i < numToJoin; i++)
        relNamesSet.insert(relNames[i]);

    for (relNamesSetIterator = relNamesSet.begin(); relNamesSetIterator != relNamesSet.end(); relNamesSetIterator++)
    {
        //get the set to which current relation from relNames belongs
        bool relationExistsInJoinList = false;

        //Note: the list keeps on getting shorter as subsets that completely match --> get removed
        for (joinListItreator = joinList.begin(); joinListItreator != joinList.end(); joinListItreator++)
        {
            subset = *joinListItreator;
            if (subsetIterator != subset.end())
            { //check all member of this subset against relNamesSet
                relationExistsInJoinList = true;
                for (subsetIterator = subset.find(*relNamesSetIterator); subsetIterator != subset.end(); subsetIterator++)
                {
                    if (relNamesSet.find(*subsetIterator) == relNamesSet.end())
                    {
                        cerr << "can't predict join, subset mismatch" << endl;
                        exit(1);
                    }
                    //remember to remove found one's from the relNamesSet
                    relNamesSet.erase(*subsetIterator);
                    if (fromApply)
                        joinList.remove(*joinListItreator);
                }
            }
        }
        if (!relationExistsInJoinList)
        {
            cerr << "can't predict join, a relation doesn't even exist!" << endl;
            exit(1);
        }
    }
}
void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    unordered_set<string> subset;
    validateJoin(parseTree, relNames, numToJoin, true);
    //everything went fine, so we'll be able to predict the join output for relNames
    for (int i = 0; i < numToJoin; i++)
        subset.insert(relNames[i]);

    //add the bigger set of relNames to the joinList
    joinList.push_front(subset);
    //TODO - remove these coments when everything done
    //get the set in which your rel is a member
    //loop through all the other relNames, and check if complete membership of that set is satisfied
    //if yes -->remove these rels from relNames & maintain the satisfying set as a temp
    //loop through all the other relNames
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double estimatedTuples = 0.0;
    validateJoin(parseTree, relNames, numToJoin, false);
    if (parseTree == NULL)
    {
        if (numToJoin == 1)
        {
            //selection
            return relationMap.find(relNames[0])->second.numTuples;
        }
        else
        {

            double result = 1.0;
            //cross product bw all relNames
            for (int i = 0; i < numToJoin; i++)
            {
                result *= relationMap.find(relNames[i])->second.numTuples;
            }
            return result;
        }
    }
}
