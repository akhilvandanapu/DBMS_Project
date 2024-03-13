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
selectAttributes = "cust,prod,avg_quant,max_quant,count_quant"
groupingVarCount = 0
groupingAttributes = "cust,prod"
fVect = "avg_quant,max_quant,min_quant,count_quant"
predicates = ""
havingCondition = ""
MF_Struct = {}


# Code for basic SQL Query:
for row in query:
	key = '' #to store in mfstruct
	value = {} 
	for attr in groupingAttributes.split(','): #create key from grouping attributes of the current row in the table
		key += f'{str(row[attr])},'
	key = key[:-1]  
	if key not in MF_Struct.keys(): #if the key is not in the MF Struct, create a new entry for the MF Struct
		for groupAttr in groupingAttributes.split(','):
			colVal = row[groupAttr]
			if colVal:
				value[groupAttr] = colVal
		#we loop through the aggregate functions list and initialize the values
		for fVectAttr in fVect.split(','):
			tableCol = fVectAttr.split('_')[1]
			if (fVectAttr.split('_')[0] == 'avg'): 
				#average is stored as a dictionary to store avg,sum and count and for each row it is updated
				value[fVectAttr] = {'sum': row[tableCol], 'count': 1, 'avg': row[tableCol]}
			elif (fVectAttr.split('_')[0] == 'count'):
				value[fVectAttr] = 1
			else:
				value[fVectAttr] = row[tableCol]
		MF_Struct[key] = value #insert new row into the MFStruct
	else: #If the row is already present in the MFStruct, we will just update it
		for fVectAttr in fVect.split(','):
			tableCol = fVectAttr.split('_')[1]
			if (fVectAttr.split('_')[0] == 'sum'):
				MF_Struct[key][fVectAttr] += int(row[tableCol]) #update the sum for that row
			elif (fVectAttr.split('_')[0] == 'avg'):
				newSum = MF_Struct[key][fVectAttr]['sum'] + int(row[tableCol])
				newCount = MF_Struct[key][fVectAttr]['count'] + 1
				MF_Struct[key][fVectAttr] = {'sum': newSum, 'count': newCount, 'avg': newSum / newCount}
			elif (fVectAttr.split('_')[0] == 'count'):
				MF_Struct[key][fVectAttr] += 1
			elif (fVectAttr.split('_')[0] == 'min'): #check if the row's quant is a new min compared to the corresponding row of the MFStruct
				if row[tableCol] < MF_Struct[key][fVectAttr]:
					MF_Struct[key][fVectAttr] = int(row[tableCol])
			else: #check if that row's quant is new max, then update it
				if row[tableCol] > MF_Struct[key][fVectAttr]:
					MF_Struct[key][fVectAttr] = int(row[tableCol])
#We generate output table using Pretty table for formatted look
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
					if len(string.split('_')) > 1 and string.split('_')[0] == 'avg':
						evalString += str(MF_Struct[row][string]['avg']) #if the string is avg, we update the eval string with avg value
					else:
						evalString += str(MF_Struct[row][string])
			else:
				evalString += f' {string} '
		if eval(evalString.replace('=', '==')):
			row_info = []
			for val in selectAttributes.split(','):
				if len(val.split('_')) > 1 and val.split('_')[0] == 'avg':
					row_info += [str(MF_Struct[row][val]['avg'])]
				else:
					row_info += [str(MF_Struct[row][val])]
			output.add_row(row_info)
		evalString = ''
	else:
        #If there is no Having condition, we add all the rows of MFStruct to the output table
		row_info = []
		for val in selectAttributes.split(','):
			if len(val.split('_')) > 1 and val.split('_')[0] == 'avg':
				row_info += [str(MF_Struct[row][val]['avg'])]
			else:
				row_info += [str(MF_Struct[row][val])]
		output.add_row(row_info)
print(output)