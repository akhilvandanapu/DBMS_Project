import os
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
query.execute("SELECT * FROM sales")

# Input Variables:
selectAttributes = "prod,1_sum_quant,2_sum_quant,3_sum_quant"
groupingVarCount = 3
groupingAttributes = "prod"
fVect = "1_sum_quant,2_sum_quant,3_sum_quant"
predicates = "1.month = 1 , 2.month = 2 , 3.month = 3"
havingCondition = ""
MF_Struct = {}


# Algorithm for MF Query:
predicates = predicates.split(',')
pList = []
#We split each predicate statement and store it in a 2D array
for i in predicates:
	pList.append(i.split(' '))
for i in range(int(groupingVarCount)+1): # loop through the table to evaluate each grouping variable
    # Initial pass say 0th pass where each row of the MF Struct is initialized for each group based on the grouping variables.
    # Each row in the MF struct also has its columns initalized appropriately based on the aggregates in the aggregate functions list
	if i == 0:
		for row in query:
			key = ''
			value = {}
			for attr in groupingAttributes.split(','):
				key += f'{str(row[attr])},'
			key = key[:-1]
			if key not in MF_Struct.keys():
				for groupAttr in groupingAttributes.split(','):
					colVal = row[groupAttr]
					if colVal:
						value[groupAttr] = colVal
				for fVectAttr in fVect.split(','):
					if (fVectAttr.split('_')[1] == 'avg'):
                        # Average is saved as a dictionary with the sum, count, and avg
						value[fVectAttr] = {'sum':0, 'count':0, 'avg':0}
					elif (fVectAttr.split('_')[1] == 'min'):
                        # Min is initialized as 7777, there is no value greater than this in the table.
						value[fVectAttr] = 7777 
					else:
                        # We initalize the values of count, sum and max to 0
						value[fVectAttr] = 0
				MF_Struct[key] = value #adds row to MF Struct
	else: #other n passes for each grouping variable 
			for aggregate in fVect.split(','):
				aggList = aggregate.split('_')
				query=conn.cursor()
				query.execute("SELECT * FROM sales")
				groupVar = aggList[0]
				aggFunc = aggList[1]
				aggCol = aggList[2]
                
				if i == int(groupVar):
					for row in query:
						key = ''
						for attr in groupingAttributes.split(','):
							key += f'{str(row[attr])},'
						key = key[:-1]
                        #checks if the aggregate function is sum 
						if aggFunc == 'sum':
                            # Create a string to replace the variables with their values
							evalString = predicates[i-1]
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If the statement is true, update the sum
							if eval(evalString.replace('=', '==')):
								sum = int(row[aggCol])
								MF_Struct[key][aggregate] += sum
                        #checks if the aggregate function is avg, if so, it will update the sum and count, then calculate the average
						elif aggFunc == 'avg':
							sum = MF_Struct[key][aggregate]['sum']
							count = MF_Struct[key][aggregate]['count']
							evalString = predicates[i-1]
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If the statement is true and count isn't 0, update the avg
							if eval(evalString.replace('=', '==')):
								sum += int(row[aggCol])
								count += 1
								if count != 0:
									MF_Struct[key][aggregate] = {'sum': sum, 'count': count, 'avg': (sum/count)}
                        #checks if the aggregate function is min, if so, it will update the min
						elif aggFunc == 'min':
							# check if row meets predicate requirements
							evalString = predicates[i-1]
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If the statement is true, update the min
							if eval(evalString.replace('=', '==')):
								min = int(MF_Struct[key][aggregate])
								if int(row[aggCol]) < min:
									MF_Struct[key][aggregate] = row[aggCol]
                        #checks if the aggregate function is max, if so, it will update the max
						elif aggFunc == 'max':
							# check if row meets predicate requirements
							evalString = predicates[i-1]
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If the statement is true, update the max
							if eval(evalString.replace('=', '==')):
								max = int(MF_Struct[key][aggregate])
								if int(row[aggCol]) > max:
									MF_Struct[key][aggregate] = row[aggCol]
                        #checks if the aggregate function is count, if so, it will update the count
						elif aggFunc == 'count':
							# check if row meets predicate requirements
							evalString = predicates[i-1]
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
							if eval(evalString.replace('=', '==')): # If evalString is true, increment the count
								MF_Struct[key][aggregate] += 1
#Generate output table using Pretty table for formatted look
output = PrettyTable()
output.field_names = selectAttributes.split(',')
for row in MF_Struct:
	evalString = ''
	if havingCondition != '':
        #If there are conditions in Having clause, we loop through all the elements of it 
		for string in havingCondition.split(' '):
			if string not in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:
				try:
					int(string)
					evalString += string
				except:
					if len(string.split('_')) > 1 and string.split('_')[1] == 'avg':
						evalString += str(MF_Struct[row][string]['avg'])
					else:
						evalString += str(MF_Struct[row][string])
			else:
				evalString += f' {string} '
		if eval(evalString.replace('=', '==')):
			row_info = []
			for val in selectAttributes.split(','):
				if len(val.split('_')) > 1 and val.split('_')[1] == 'avg':
					row_info += [str(MF_Struct[row][val]['avg'])]
				else:
					row_info += [str(MF_Struct[row][val])]
			output.add_row(row_info)
		evalString = ''
	else:
        #If there is no Having condition, we add all the rows of MFStruct to the output table
		row_info = []
		for val in selectAttributes.split(','):
			if len(val.split('_')) > 1 and val.split('_')[1] == 'avg':
				row_info += [str(MF_Struct[row][val]['avg'])]
			else:
				row_info += [str(MF_Struct[row][val])]
		output.add_row(row_info)
print(output) # print the output table in formatted look 