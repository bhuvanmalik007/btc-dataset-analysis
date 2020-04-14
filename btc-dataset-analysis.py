import pandas as pd
import sys
import csv

# Question 1
print("\n*****Question 1*****")
txhImport = pd.read_csv("./txh.dat", sep='\s\s+', header=None, engine='python')
noOfTransactions = txhImport.shape[0]
print("number of transactions: ")
print(noOfTransactions)

addressesImport = pd.read_csv(
    "./addresses.dat", sep='\s\s+', header=None, engine='python')
noOfAddresses = len(addressesImport) + 1
# row_count = addressesImport.shape[0]
print("number of addresses: ")
print(noOfAddresses)

# Question 2
print("\n*****Question 2*****")
txinImport = pd.read_csv("./txin.dat", sep='\s+',
  names = ['addrID', 'txinSum'], usecols = [4,5])
txoutImport = pd.read_csv("./txout.dat", sep='\s+',
  names = ['addrID', 'txoutSum'], usecols = [2,3])
addressesImport = pd.read_csv('./addresses.dat', delim_whitespace = True,
  header = None, engine = 'python', names = ['addrID','address'], usecols= [0,1])

txoutImportGrouped = txoutImport.groupby(['addrID']).sum()
txinImportGrouped = txinImport.groupby(['addrID']).sum()

merged = pd.merge(txoutImportGrouped, txinImportGrouped, how='outer', on='addrID')
merged.fillna(0, inplace=True)

merged['UXTO'] = merged['txoutSum'] - merged['txinSum']

utxoMax = merged['UXTO'].max()
utxoMaxRow = merged[merged['UXTO'] == utxoMax]
utxoMaxRowAddressId = utxoMaxRow.index.values[0]
utxoMaxRowAddress = addressesImport[addressesImport['addrID'] == utxoMaxRowAddressId]['address'].values[0]

print("Address ID of address holding max bitcoin: ", utxoMaxRowAddressId)
print("Address holding max bitcoin: ", utxoMaxRowAddress)
print("The max balance is: ", utxoMax)


# Question 3
print("\n*****Question 3*****")
txoutImport = pd.read_csv("./txout.dat", sep="\s+", names = ['addrID', 'txoutSum'], usecols=[2,3])
addressesImport = pd.read_csv('./addresses.dat', delim_whitespace = True, header = None, engine = 'python',
names = ['addrID','address'], usecols= [0,1])
txinImport = pd.read_csv("./txin.dat", sep="\s+", names = ['addrID', 'txinSum'], usecols=[4,5])

txinImport_group = txinImport.groupby(['addrID']).sum()
txoutImport_group = txoutImport.groupby(['addrID']).sum()

merged = pd.merge(txoutImport_group, txinImport_group, how='outer', on='addrID')
merged.fillna(0, inplace=True)
merged['UXTO'] = merged['txoutSum'] - merged['txinSum']

mergedSum = merged['UXTO'].sum()
mergedLength = merged.shape[0]
print("Answer: ", (mergedSum/mergedLength))


# Question 4
print("\n*****Question 4*****")
txhImport = pd.read_csv("./txh.dat", sep='\s\s+', header=None, engine='python')
noOfTransactions = txhImport.shape[0]

addressesImport = pd.read_csv("./addresses.dat",sep='\s\s+', header=None, engine='python')
noOfAddresses = len(addressesImport)
print("Average number of transactions per address: ", noOfTransactions/noOfAddresses)

txinImport = pd.read_csv("txin.dat", sep="\s+", names = ['txID','addrID'], usecols=[0, 4])
txinTotal = len(txinImport)

txoutImport = pd.read_csv("txout.dat", sep="\s+", names = ['txID','addrID'],usecols=[0,2])
txoutTotal = len(txoutImport)

print("Average number of input and output transactions per address: ", float(txinTotal)/float(noOfAddresses),", ", float(txoutTotal)/float(noOfAddresses))

# Question 5
print("\n*****Question 5*****")
maximumInputs = 0
with open('./tx.dat') as txImport:
    txCsv = csv.reader(txImport, delimiter="\t")
    for row in txCsv:
        if maximumInputs < int(row[2]):
            maximumInputs = int(row[2])
txID = []
with open('./tx.dat') as txImport:
    txCsv = csv.reader(txImport, delimiter="\t")
    for row in txCsv:
        if maximumInputs == int(row[2]):
            txID.append(row[0])
print("Transaction ID with greatest number of inputs: ", txID)
print("Total number of inputs in the transaction: ", maximumInputs)
with open('./txh.dat') as txhImport:
    txhCsv = csv.reader(txhImport, delimiter="\t")
    for row in txhCsv:
        if row[0] in txID:
            print("Hash of that Transaction: ", row[1])

# Question 6
print("\n*****Question 6*****")
txhImport = pd.read_csv("./txh.dat",sep='\s\s+', header=None, engine='python')
txnNum = len(txhImport)
txoutImport = pd.read_csv("./txout.dat", sep="\s+", names = ['addrID','txoutSum'], usecols=[2,3])

txoutImportGrouped = txoutImport.groupby(['addrID']).sum()
txnTotalSum = txoutImportGrouped['txoutSum'].sum()

txnAvg = float(txnTotalSum)/float(txnNum)
print("The average transaction value is: ", txnAvg)


# Question 7
print("\n*****Question 7*****")
cbTransactions = 0
with open('./tx.dat') as txImport:
    txCsv = csv.reader(txImport, delimiter = "\t")
    for row in txCsv:
        if (row[2] == '0'):
          cbTransactions = 1 + cbTransactions
print("Number of coinbase transactions: ", cbTransactions)


# Question 8
print("\n*****Question 8*****")
transactionsTotal = 0
blocksTotal = 212576
with open('./bh.dat') as bhImport:
    bhCsv = csv.reader(bhImport, delimiter = "\t")
    for row in bhCsv:
        transactionsTotal += int(row[3])
print("Average number of transactions per block: ", (transactionsTotal/blocksTotal))
