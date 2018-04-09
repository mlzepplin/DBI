#include "Statistics.h"
#include "cstring"
#include "iostream"

Statistics::Statistics()
{
    joinList = new list<unordered_set<string>>();
    relationMap = new unordered_map<string, RelationInfo>();
}

Statistics::Statistics(Statistics &copyMe)
{ //copy constructor, creating a deep copy

    for (unordered_map<string, RelationInfo>::iterator relItr = copyMe.relationMap.begin(); relItr != copyMe.relationMap.end(); relItr++)
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

        relationMap.insert(pair<std::string, RelationInfo>(relName, relationInfo));
        relationInfo.attributeMap.clear();
    }
}

Statistics::~Statistics()
{
    delete joinList;
    delete relationMap;
}

void Statistics::AddRel(char *relName, int numTuples)
{
    if (relationMap->find(relName) == relationMap->end())
    {
        //create empty attribute map
        unordered_map<string, int> lineAttributeMap;

        //insert into relation Map
        RelationInfo relationInfo = {numTuples, tempAttributeMap};
        relationMap->insert(std::make_pair(relName, relationInfo));

        //populating join list with singleton
        unordered_set<string> tempRel;
        tempRel.insert(relName);
        joinList->push_front(tempRel);

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
    relationMap.clear();
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
        if (!strcmp(line, "Relation"))
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
            relationMap.insert(pair<string, RelationInfo>(relName, relationInfo));
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
        char *relName = new char[relrelItr->first.length() + 1];
        strcpy(relName, relrelItr->first.c_str());
        fprintf(statisticsInfo, "Relation\n%s \n", relName);
        fprintf(statisticsInfo, "%d tuples\n", relrelItr->second.numTuples);
        fprintf(statisticsInfo, "Attributes\n";
        
        //Loop through attribute map
        for (unordered_map<std::string, int>::iterator attItr = relItr->second.attributeMap.begin(); attItr != relItr->second.attributeMap.end(); attItr++)
        {
            char *attName = new char[attItr->first.length() + 1];
            strcpy(attName, attrelItr->first.c_str());
            fprintf(statisticsInfo, "%s\n", attName);
            fprintf(statisticsInfo, "%d\n", attItr->second);
        }
    }
    fprintf(statisticsInfo, "eof");
    fclose(statisticsInfo);
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
        relMapIter = relationMap->find(relNames[i]);
        if (relMapIter == relationMap->end())
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

        break;
    }
    numDistincts = attMapIter->second;
    return relMapIter->second.numTuples;
}

void Statistics::validateJoin(struct AndList *parseTree, char *relNames[], int numToJoin, bool fromApply)
{ //assumes joinList is populated

    unordered_set<string>::iterator subsetIterator, relNamesSetIterator;

    list<unordered_set<string>>::iterator joinListItreator;
    unordered_set<string> *relNamesSet, subset;
    relNamesSet = new unordered_set<string>();

    struct AndList *currentAnd = parseTree;
    struct OrList *currentOr = currentAnd->left;

    if (!checkAttributes(parseTree, relNames, numToJoin))
    {
        cerr << "either attribute not present in relNames or relNames is not a subset of relationMap" << endl;
        exit(1);
    }
    //making a set version of relNames, to reduce internal lookups to O(1)
    for (int i = 0; i < numToJoin; i++)
        relNamesSet->insert(relNames[i]);

    // for(int i=0;i<joinList.size();i++){

    // }

    for (relNamesSetIterator = relNamesSet->begin(); relNamesSetIterator != relNamesSet->end();)
    {

        //get the set to which current relation from relNames belongs
        bool relationExistsInJoinList = false;

        //Note: the list keeps on getting shorter as subsets that completely match --> get removed
        for (joinListItreator = joinList->begin(); joinListItreator != joinList->end();)
        {
            //take a subset from the joinList

            subset = *joinListItreator;

            //if current relNames exists in this subset
            if (relNamesSetIterator == relNamesSet->end())
                return;
            subsetIterator = subset.find(*relNamesSetIterator);
          
            if (subsetIterator != subset.end())
            {
                relationExistsInJoinList = true;
                //check all member of this subset against relNamesSet
                for (subsetIterator = subset.begin(); subsetIterator != subset.end(); subsetIterator++)
                {

                    cout << " val join check" << endl;
                    //if unable to locate even a single one
                    if (relNamesSet->find(*subsetIterator) == relNamesSet->end())
                    {
                        cerr << "can't predict join, subset mismatch" << endl;
                        exit(1);
                    }

                    //remember to remove the found one's from the relNamesSet
                    relNamesSetIterator = relNamesSet->erase(relNamesSetIterator);

                }
                if (fromApply)
                    joinListItreator = joinList->erase(joinListItreator);
                else
                    ++joinListItreator;
            }
        }
        if (!relationExistsInJoinList)
        {
            cerr << "can't predict join relation " << *relNamesSetIterator << " doesn't even exist!" << endl;
            exit(1);
        }
    }
}

double Statistics::fractionise(int numTuples, int numDistincts)
{
    return (numTuples - numDistincts) / (double)numTuples;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    unordered_set<string> subset;
    validateJoin(parseTree, relNames, numToJoin, true);

    //everything went fine, so we'll be able to predict the join output for relNames
    for (int i = 0; i < numToJoin; i++)
        subset.insert(relNames[i]);

    //add the bigger set of relNames to the joinList
    joinList->push_front(subset);
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double estimatedTuples = 1.0;

    validateJoin(parseTree, relNames, numToJoin, false);
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
    double minAndFraction = 1.0;
    while (currentAnd->rightAnd != NULL)
    {
        currentOr = currentAnd->left;

        double maxOrFraction = 0.0;
        while (currentOr->rightOr != NULL)
        {
            currentComparisonOp = currentOr->left;
            leftOperand = currentComparisonOp->left;
            rightOperand = currentComparisonOp->right;
            int numRightDistincts = 0, numLeftDistincts = 0;
            int rightTuples = 0, leftTuples = 0;
            double currentOrFraction = 0.0;
            if (currentComparisonOp->code == EQUALS)
            {
                leftTuples = getNumTuples(leftOperand->value, relNames, numToJoin, numLeftDistincts);

                if (rightOperand->code == NAME)
                {
                    rightTuples = getNumTuples(rightOperand->value, relNames, numToJoin, numRightDistincts);
                    currentOrFraction = std::min(fractionise(rightTuples, numRightDistincts), fractionise(leftTuples, numLeftDistincts));
                    maxOrFraction = std::max(maxOrFraction, currentOrFraction);
                }
                else
                {
                    maxOrFraction = std::max(maxOrFraction, fractionise(leftTuples, numLeftDistincts));
                }
            }
            else
            { //less than or greater than condition
                maxOrFraction = std::max(maxOrFraction, 1.0 / 3);
            }
        }
        minAndFraction = std::min(minAndFraction, maxOrFraction);
    }
    estimatedTuples = maxJoinTuples * minAndFraction;
    return estimatedTuples;
}
