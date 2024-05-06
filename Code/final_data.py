with open('../Data/adult.data', 'r') as file1:
    data1 = file1.read()

with open('../Data/adult.test', 'r') as file2:
    # Read the first line and discard it
    next(file2)
    # Read the rest of the file
    data2 = file2.read()

with open('../Data/processed_data.txt', 'w') as outfile:
    outfile.write(data1 + '\n' + data2)