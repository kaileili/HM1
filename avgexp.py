from operator import add
import sys
from pyspark import SparkContext

def splitFunc(row):
	col_Continent, col_GNP, col_LifeExpectancy = row.split(',')
	if col_GNP > 10000
		return col_Continent, Avg(col_LifeExpectancy)

if __name__ == "__main__":
	input_file_path = "./country.csv"
	output_file_path = "./output.txt"
	# input_file_path = sys.argv[1]
	# output_file_path = sys.argv[2]
	sc = SparkContext.getOrCreate()

	lines = sc.textFile(input_file_path)

	result = lines.map(splitFunc) \
		.filter(lambda row: row is not None) \
		.mapValues(lambda value: 1 if value <= 5 else 0) \
		.reduceByKey(add) \
		.map(lambda row: str(row[0]) + "\t" + str(row[1]) + "\n").collect()


	output_file = open(output_file_path, "w+")
	output_file.writelines(result)
	output_file.close()