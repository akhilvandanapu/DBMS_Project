function = """for row in query:
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
print(output)"""
def helpersqlQuery():
	with open('generated.py', 'a') as generatedFile:
		generatedFile.write(function)
		generatedFile.close()


function1 = """predicates = predicates.split(',')
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
print(output) # print the output table in formatted look """
def helpermfQuery():
    with open('generated.py', 'a') as generatedFile:
        generatedFile.write(function1)
        generatedFile.close()

function3 = """predicates = predicates.split(',') # we split each predicate statement and store it in a 2D array
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
print(output)"""
def helperemfQuery():
    with open('generated.py', 'a') as generatedFile:
        generatedFile.write(function3)
        generatedFile.close()