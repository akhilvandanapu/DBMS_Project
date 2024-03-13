import subprocess
from helper import helpersqlQuery 
from helper import helpermfQuery 
from helper import helperemfQuery 

def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    """
    # Receive Input from user

inputType = input("Please enter the file name for reading input from file, or do enter to input the variables inline: ")
selectAttributes = ""
groupingVarCount = ""
groupingAttributes = ""
fVect = ""
predicates = ""
havingCondition = ""

if inputType != "":
    with open(inputType) as f:
        content = f.readlines()
    content = [x.rstrip() for x in content]
    i = 0
    while i < len(content):
        if content[i] == "SelectAttributes:":
            i += 1
            selectAttributes = content[i].replace(" ", "")
            i += 1
        elif content[i] == "NumberOfGroupingVariables:":
            i += 1
            groupingVarCount = content[i].replace(" ", "")
            i += 1
        elif content[i] == "GroupingAttributes:":
            i += 1
            groupingAttributes = content[i].replace(" ", "")
            i += 1
        elif content[i] == "AggregateFunctionList:":
            i += 1
            fVect = content[i].replace(" ", "")
            i += 1
        elif content[i] == "GroupingVariableRange:":
            i += 1
            predicates = content[i]
            i += 1
        elif content[i] == "HavingCondition:":
            i += 1
            havingCondition = content[i]
            i += 1
        else:
            predicates += "," + content[i]
            i += 1
    # Remove white space from input
    selectAttributes = selectAttributes.replace(" ", "")
    groupingVarCount = groupingVarCount.replace(" ", "")
    groupingAttributes = groupingAttributes.replace(" ", "")
    fVect = fVect.replace(" ", "")
    predicates = predicates  # white space needed here to evaluate each predicate
    havingCondition = (havingCondition ) # white space needed to evaluate each having condition
    

else:
    # read input from user inline
    selectAttributes = input("Please input the select_attributes seperated by a comma: " ).replace(" ", "")
    groupingVarCount = input("Please input the number of grouping_variables: ").replace( " ", "" )
    groupingAttributes = input("Please input the grouping_attributes for the query seperated by a comma: ").replace(" ", "")
    fVect = input("Please input the list of aggregate_functions for the query seperated by a comma: " ).replace(" ", "")
    predicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma. Each predicate must have each element separated by a space: " )
    havingCondition = input("Please input the having condition with each element separated by spaces, and the AND and OR written in lowercase: ")

tmp = f"""import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
from prettytable import PrettyTable
# THIS IS GENERATED FILE 

load_dotenv()
user = os.getenv('USER')
password = os.getenv('PASSWORD')
dbname = os.getenv('DBNAME')
conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
query = conn.cursor()
query.execute("SELECT * FROM sales")"""


with open("generated.py", "w") as generatedFile:
    generatedFile.write(tmp)  # write the first part(tmp) of the code to the file
    generatedFile.write("\n\n# Input Variables:\n")
    generatedFile.write(
        f"""selectAttributes = "{selectAttributes}"\ngroupingVarCount = {groupingVarCount}\ngroupingAttributes = "{groupingAttributes}"\nfVect = "{fVect}"\npredicates = "{predicates}"\nhavingCondition = "{havingCondition}"\n""")  # write input variables to file
    generatedFile.write("MF_Struct = {}\n") # create MF_Struct dictionary and initilize it to an empty dictionary
    mf = 1  # flag to tell if a query is an MF Query or a basic SQL Query
    if (groupingVarCount == "0"): # If there are no grouping variables, it is evaluated as a regular SQL Query
        mf = 0  # setting flag to 0 as the query is basic sql query
        generatedFile.write("\n\n# Code for basic SQL Query:\n")
        generatedFile.close()  
        helpersqlQuery()  # calls basic sqlQuery function 

    for pred in predicates.split(","):  # loop through the list of ranges of grouping variables statments to see if there is a grouping attribute referenced that is not attributed to a grouping variable
        if (mf): 
            for string in pred.split(" "): # loop through each element of the predicate statment
                if string in groupingAttributes.split(","):  # if there is a grouping attribute in the predicate statment, the query is an EMF Query
                    mf = 0  # Query is an EMF Query, update flag
                    generatedFile.write("\n\n# Algorithm for EMF Query:\n")
                    generatedFile.close()
                    helperemfQuery()  # calls helperemfQuery function 
                    break  # as it is EMF,break
        else:
            break  # break it is SQL or EMF Query

    if mf:  # If query is not a basic SQL or EMF Query, it evaluates as MF Query
        generatedFile.write("\n\n# Algorithm for MF Query:\n")
        generatedFile.close()
        helpermfQuery()  # calls helpermfQuery function


if "__main__" == __name__:
    main()
