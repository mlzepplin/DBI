#include "Statistics.h"
#include "cstring"
#include "iostream"
#include <sstream>

Statistics::Statistics()
{
    relationMap = new unordered_map<string, RelationInfo>();
}

Statistics::Statistics(Statistics &copyMe)
{ //copy constructor, creating a deep copy

    for (unordered_map<string, RelationInfo>::iterator relItr = copyMe.relationMap->begin(); relItr != copyMe.relationMap->end(); relItr++)
    {
        string relName = relItr->first;
        RelationInfo relationInfo;
        relationInfo.numTuples = relItr->second.numTuples;

        for (unordered_map<std::string, int>::iterator attItr = relItr->second.attributeMap.begin(); attItr != relItr->second.attributeMap.end(); attItr++)
        {
            string attName = attItr->first;
            int numDistinct = attItr->second;
            relationInfo.attributeMap.insert(pair<std::string, int>(attName, numDistinct));
        }

        relationMap->insert(pair<std::string, RelationInfo>(relName, relationInfo));
        relationInfo.attributeMap.clear();
    }
}

Statistics::~Statistics()
{
    delete relationMap;
}

void Statistics::AddRel(char *relName, int numTuples)
{
    if (relationMap->find(relName) == relationMap->end())
    {
        //create empty attribute map
        unordered_map<string, int> tempAttributeMap;

        //insert into relation Map
        RelationInfo relationInfo = {numTuples, tempAttributeMap};
        relationMap->insert(std::make_pair(relName, relationInfo));

        //populating join list with singleton
        unordered_set<string> tempRel;
        tempRel.insert(relName);
    }
    else
    { //update numTuples
        relationMap->find(relName)->second.numTuples = numTuples;
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    //need to find the relation first
    if (relationMap->find(relName) == relationMap->end())
    {
        cerr << "the relation to which you're trying to add attribute, does not exist!!" << endl;
        exit(1);
    }
    else
    {
        RelationInfo *relationInfo = &relationMap->find(relName)->second;

        //If numDistincts is initially passed in as a –1, then the number of distincts is assumed to be equal to the number of tuples in the associated relation.
        if (numDistincts == -1)
            numDistincts = relationInfo->numTuples;

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
    RelationInfo relationInfo = relationMap->find(oldName)->second;

    if (relationMap->find(newName) != relationMap->end())
        relationMap->insert(std::make_pair(newName, relationInfo));
    else
    {
        cerr << "relation wih newName already exists" << endl;
        exit(1);
    }
}

void Statistics::Read(char *fromWhere)
{
    relationMap->clear();
    FILE *statisticsInfo;
    statisticsInfo = fopen(fromWhere, "r");

    //If the file does not exist , create an empty file instead of throwing an error
    if (statisticsInfo == NULL)
    {
        statisticsInfo = fopen(fromWhere, "w");
        fprintf(statisticsInfo, "eof");
        fclose(statisticsInfo);
        statisticsInfo = fopen(fromWhere, "r");
    }

    char line[200], rel[200];

    fscanf(statisticsInfo, "%s", line);

    while (strcmp(line, "eof") != 0)
    {
        if (!strcmp(line, "Relation:"))
        {
            RelationInfo relationInfo;
            fscanf(statisticsInfo, "%s", line);
            string relName(line);
            strcpy(rel, relName.c_str());
            fscanf(statisticsInfo, "%s", line);
            relationInfo.numTuples = atoi(line);
            fscanf(statisticsInfo, "%s", line);
            fscanf(statisticsInfo, "%s", line);

            while (strcmp(line, "relation") != 0 && strcmp(line, "eof") != 0)
            {
                int numDistinct;
                string attName(line);
                fscanf(statisticsInfo, "%s", line);
                numDistinct = atoi(line);
                relationInfo.attributeMap.insert(pair<string, int>(attName, numDistinct));
                fscanf(statisticsInfo, "%s", line);
            }
            relationMap->insert(pair<string, RelationInfo>(relName, relationInfo));
        }
    }
    fclose(statisticsInfo);
}

void Statistics::Write(char *fromWhere)
{

    FILE *statisticsInfo;
    statisticsInfo = fopen(fromWhere, "w");

    //Loop through the relation map
    for (unordered_map<std::string, RelationInfo>::iterator relItr = relationMap->begin(); relItr != relationMap->end(); relItr++)
    {
        char *relName = new char[relItr->first.length() + 1];
        strcpy(relName, relItr->first.c_str());
        fprintf(statisticsInfo, "Relation:\n%s \n", relName);
        fprintf(statisticsInfo, "%d tuples\n", relItr->second.numTuples);
        fprintf(statisticsInfo, "Attributes:\n");

        //Loop through attribute map
        for (unordered_map<std::string, int>::iterator attItr = relItr->second.attributeMap.begin(); attItr != relItr->second.attributeMap.end(); attItr++)
        {
            char *attName = new char[attItr->first.length() + 1];
            strcpy(attName, attItr->first.c_str());
            fprintf(statisticsInfo, "%s\n", attName);
            fprintf(statisticsInfo, "%d\n", attItr->second);
        }
    }
    fprintf(statisticsInfo, "eof");
    fclose(statisticsInfo);
}

unordered_set<string> Statistics::checkAttributes(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr;
    struct ComparisonOp *currentComparisonOp;
    struct Operand *leftOperand, *rightOperand;
    unordered_set<string> matchedRelNames;
    string attName;
    if (parseTree == NULL)
        return matchedRelNames; //return empty

    do
    {
        currentOr = currentAnd->left;
        do
        {
            currentComparisonOp = currentOr->left;
            leftOperand = currentComparisonOp->left;
            rightOperand = currentComparisonOp->right;
            if (leftOperand->code == NAME)
            {
                attName = leftOperand->value;

                matchedRelNames.insert(findAttInRelation(attName, relNames, numToJoin));

                if (rightOperand->code == NAME)
                {
                    attName = rightOperand->value;
                    matchedRelNames.insert(findAttInRelation(attName, relNames, numToJoin));
                }
            }
            currentOr = currentOr->rightOr;
        } while (currentOr != NULL);
        currentAnd = currentAnd->rightAnd;
    } while (currentAnd != NULL);
    return matchedRelNames;
}

string Statistics::findAttInRelation(string attName, char *relNames[], int numToJoin)
{
    unordered_map<string, RelationInfo>::iterator relMapIter;
    unordered_map<string, int>::iterator attMapIter;
    unordered_map<string, int> attMap;

    for (int i = 0; i < numToJoin; i++)
    {
        relMapIter = relationMap->find(relNames[i]);
        if (relMapIter == relationMap->end())
            exit(1);
        else
        {

            attMap = relMapIter->second.attributeMap;
            attMapIter = attMap.find(attName);
            if (attMapIter == attMap.end())
                continue;
            else
            { //return matched rel name
                return relMapIter->first;
            }
        }
    }
    exit(1);
}

int Statistics::getNumTuples(string attName, char *relNames[], int numToJoin, int &numDistincts)
{ //assumes that attribute does exist in atleast one relation
    unordered_map<string, RelationInfo>::iterator relMapIter;
    unordered_map<string, int>::iterator attMapIter;
    unordered_map<string, int> attMap;

    for (int i = 0; i < numToJoin; i++)
    {
        relMapIter = relationMap->find(relNames[i]);

        attMap = relMapIter->second.attributeMap;
        attMapIter = attMap.find(attName);

        if (attMapIter == attMap.end())
            continue;
        else
            break;
    }
    numDistincts = attMapIter->second;
    return relMapIter->second.numTuples;
}

vector<string> Statistics::tokeniseKey(string input)
{
    vector<string> tokens;
    stringstream check1(input);
    string intermediate;
    while (getline(check1, intermediate, '|'))
    {
        tokens.push_back(intermediate);
    }
    return tokens;
}

unordered_set<string> Statistics::validateJoin(struct AndList *parseTree, char *relNames[], int numToJoin)
{ //assumes joinList is populated

    unordered_set<string>::iterator relNamesSetIter;
    unordered_set<string> *relNamesSet = new unordered_set<string>();
    unordered_map<string, RelationInfo>::iterator relMapIter;
    unordered_set<string> matchedRelNamesSet;
    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr = currentAnd->left;

    matchedRelNamesSet = checkAttributes(parseTree, relNames, numToJoin);

    //making a set version of relNames, to reduce internal lookups to O(1)
    for (int i = 0; i < numToJoin; i++)
        relNamesSet->insert(relNames[i]);

    for (relNamesSetIter = relNamesSet->begin(); relNamesSetIter != relNamesSet->end();)
    {

        //get the set to which current relation from relNames belongs
        bool relationExistsInRelationMap = false;

        //Note: the list keeps on getting shorter as subsets that completely match --> get removed
        for (relMapIter = relationMap->begin(); relMapIter != relationMap->end(); ++relMapIter)
        {

            //if current relNames is a substring of current relMapIter's key
            if (relNamesSetIter == relNamesSet->end())
                return matchedRelNamesSet; //update to add iterator returns

            //get the key string of relMapIter
            string relMapKey = relMapIter->first;

            //if current relNames belongs to some relationMap key
            if (relMapKey.find(*relNamesSetIter) != std::string::npos)
            {
                relationExistsInRelationMap = true;

                //get a delimited vector of strings, iterate and lookup
                vector<string> tokens = tokeniseKey(relMapKey);
                //lookup all substrings of this relationMap key in relNamesSet
                for (int i = 0; i < tokens.size(); i++)
                {

                    //if unable to locate even a single one
                    if (relNamesSet->find(tokens[i]) == relNamesSet->end())
                    {
                        cerr << "can't predict join, subset mismatch" << endl;
                        exit(1);
                    }

                    //remember to remove the found one's from the relNamesSet
                    relNamesSetIter = relNamesSet->erase(relNamesSetIter);
                }
            }
        }
        if (!relationExistsInRelationMap)
        {
            cerr << "can't predict join relation " << *relNamesSetIter << " doesn't even exist!" << endl;
            exit(1);
        }
    }
    return matchedRelNamesSet;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    unordered_set<string> matchedRelSet;
    unordered_set<string>::iterator matchedRelSetIter;
    unordered_map<string, int> attMap;
    unordered_map<string, int>::iterator attMapIter;
    matchedRelSet = validateJoin(parseTree, relNames, numToJoin);
    char *newKey = new char[200];

    //everything went fine, so we'll be able to predict the join output for relNames
    for (int i = 0; i < numToJoin; i++)
        sprintf(newKey, "%s|%s", newKey, relNames[i]);

    //add the bigger set of relNames to the joinList
    //create an attmap of new join
    double estimate = Estimate(parseTree, relNames, numToJoin);

    AddRel(newKey, estimate);

    for (matchedRelSetIter = matchedRelSet.begin(); matchedRelSetIter != matchedRelSet.end(); matchedRelSetIter++)
    {
        //iterate over att map
        attMap = relationMap->find(*matchedRelSetIter)->second.attributeMap;
        for (attMapIter = attMap.begin(); attMapIter != attMap.end(); attMapIter++)
        { //insert all atts to the newkey relation
            relationMap->find(newKey)->second.attributeMap.insert(*attMapIter);
        }
        //delete the previous relations that got joined
        relationMap->erase(*matchedRelSetIter);
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double estimatedTuples = 1.0;

    validateJoin(parseTree, relNames, numToJoin);
    if (parseTree == NULL)
    {
        if (numToJoin == 1)
        {
            //selection
            return relationMap->find(relNames[0])->second.numTuples;
        }
        else
        {

            //double result = 1.0;
            //cross product bw all relNames
            for (int i = 0; i < numToJoin; i++)
            {
                estimatedTuples *= relationMap->find(relNames[i])->second.numTuples;
            }
            return estimatedTuples;
        }
    }
    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr;
    struct ComparisonOp *currentComparisonOp;
    struct Operand *leftOperand, *rightOperand;
    string attName;
    double maxJoinTuples = 1.0;

    //compute max possible join tuples
    for (int i = 0; i < numToJoin; i++)
    {
        maxJoinTuples *= relationMap->find(relNames[i])->second.numTuples;
    }

    //reducing the maxTuples by factoring with all the attribute conditions
    double minAndTuplesEstimate = maxJoinTuples;
    do
    {
        currentOr = currentAnd->left;

        double maxOrTuplesEstimate = 0.0;
        do
        {
            currentComparisonOp = currentOr->left;
            leftOperand = currentComparisonOp->left;
            rightOperand = currentComparisonOp->right;
            int numRightDistincts = 0, numLeftDistincts = 0;
            int rightTuples = 0, leftTuples = 0;
            double currentOrTuplesEstimate = 0.0;

            leftTuples = getNumTuples(leftOperand->value, relNames, numToJoin, numLeftDistincts);

            if (currentComparisonOp->code == EQUALS)
            {
                if (rightOperand->code == NAME)
                {
                    double numDistinctOfSmallerRelation = 1.0;
                    rightTuples = getNumTuples(rightOperand->value, relNames, numToJoin, numRightDistincts);

                    if (leftTuples > rightTuples)
                        numDistinctOfSmallerRelation = numLeftDistincts;
                    else
                        numDistinctOfSmallerRelation = numRightDistincts;

                    currentOrTuplesEstimate = ((double)leftTuples * rightTuples) / numDistinctOfSmallerRelation;

                    maxOrTuplesEstimate = std::max(maxOrTuplesEstimate, currentOrTuplesEstimate);
                }
                else
                {
                    maxOrTuplesEstimate = std::max(maxOrTuplesEstimate, leftTuples / (double)numLeftDistincts);
                }
            }
            else
            { //less than or greater than condition
                maxOrTuplesEstimate = std::max(maxOrTuplesEstimate, (1.0 / 3) * leftTuples);
            }
        } while (currentOr->rightOr != NULL);

        minAndTuplesEstimate = std::min(minAndTuplesEstimate, maxOrTuplesEstimate);

    } while (currentAnd->rightAnd != NULL);

    estimatedTuples = minAndTuplesEstimate;
    return estimatedTuples;
}
