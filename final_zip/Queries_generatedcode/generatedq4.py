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
selectAttributes = "cust,month,1_avg_quant,2_avg_quant,3_avg_quant"
groupingVarCount = 3
groupingAttributes = "cust,month"
fVect = "1_avg_quant,2_avg_quant,3_avg_quant"
predicates = "1.cust = cust and 1.month < month , 2.cust = cust and 2.month > month , 3.cust = cust and 3.month = month"
havingCondition = ""
MF_Struct = {}


# Algorithm for EMF Query:
predicates = predicates.split(',') # we split each predicate statement and store it in a 2D array
pList = []
for i in predicates:
	pList.append(i.split(' '))
for i in range(int(groupingVarCount)+1):
    #Initial pass say 0th pass where each row of the MF Struct is initialized for each group based on the grouping variables.
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
                    # Average is stored as dictionary with the sum, count, and overall average
					if (fVectAttr.split('_')[1] == 'avg'):
						value[fVectAttr] = {'sum':0, 'count':0, 'avg':0}
                    # Min is initialized as 7777, there is no value greater than this in the table.
					elif (fVectAttr.split('_')[1] == 'min'):
						value[fVectAttr] = 7777
					else:
						value[fVectAttr] = 0
				MF_Struct[key] = value
	else:
        # Other n passes for each grouping variables
		for aggregate in fVect.split(','):
			aggList = aggregate.split('_')
			query=conn.cursor()
			query.execute("SELECT * FROM sales")
			groupVar = aggList[0]
			aggFunc = aggList[1]
			aggCol = aggList[2]
            # check if the aggregate function is being called on the grouping variable you are currently on (i)
            # Also loop through every key in the MF_Struct to update every row of the MF_Struct the predicate statments apply to(1.state = state and 1.cust = cust vs 1.state = state)
			if i == int(groupVar):
				for row in query:
					for key in MF_Struct.keys():
                         #checks if the aggregate function is sum
						if aggFunc == 'sum':
							evalString = predicates[i-1]
                            # Create a string to replace the grouping variables with their values
                            # It also checks if the string is a grouping variable and replace that with its value from the table 
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
								elif string in groupingAttributes.split(','):
									rowVal = MF_Struct[key][string]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If evalString is true, update the sum
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
								elif string in groupingAttributes.split(','):
									rowVal = MF_Struct[key][string]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If evalString is true and count isn't 0, update the avg
							if eval(evalString.replace('=', '==')):
								sum += int(row[aggCol])
								count += 1
								if count != 0:
									MF_Struct[key][aggregate] = {'sum': sum, 'count': count, 'avg': (sum/count)}
                        #checks if the aggregate function is min, if so, it will update the min
						elif aggFunc == 'min':
							evalString = predicates[i-1]
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
								elif string in groupingAttributes.split(','):
									rowVal = MF_Struct[key][string]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If evalString is true, update the min
							if eval(evalString.replace('=', '==')):
								min = int(MF_Struct[key][aggregate])
								if int(row[aggCol]) < min:
									MF_Struct[key][aggregate] = row[aggCol]
                        #checks if the aggregate function is max, if so, it will update the max
						elif aggFunc == 'max':
							evalString = predicates[i-1]
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										valString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
								elif string in groupingAttributes.split(','):
									rowVal = MF_Struct[key][string]
									try:
										int(rowVal)
										valString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If evalString is true, update the max
							if eval(evalString.replace('=', '==')):
								max = int(MF_Struct[key][aggregate])
								if int(row[aggCol]) > max:
									MF_Struct[key][aggregate] = row[aggCol]
                        #checks if the aggregate function is count, if so, it will update the count
						elif aggFunc == 'count':
							evalString = predicates[i-1]
							for string in pList[i-1]:
								if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
									rowVal = row[string.split('.')[1]]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
								elif string in groupingAttributes.split(','):
									rowVal = MF_Struct[key][string]
									try:
										int(rowVal)
										evalString = evalString.replace(string, str(rowVal))
									except:
										evalString = evalString.replace(string, f"'{rowVal}'")
                            # If evalString is true, increment the count
							if eval(evalString.replace('=', '==')):
								MF_Struct[key][aggregate] += 1
#Generate output table using Pretty table for formatted look
output = PrettyTable()
output.field_names = selectAttributes.split(',')
for row in MF_Struct:
	evalString = ''
	if havingCondition != '':
		for string in havingCondition.split(' '):
            #If there are conditions in Having clause, we loop through all the elements of it
			if string not in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:
				try:
					float(string)
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
        #there is no having condition, thus every MFStruct row will be added to the output table
		row_info = []
		for val in selectAttributes.split(','):
			if len(val.split('_')) > 1 and val.split('_')[1] == 'avg':
				row_info += [str(MF_Struct[row][val]['avg'])]
			else:
				row_info += [str(MF_Struct[row][val])]
		output.add_row(row_info)
print(output)