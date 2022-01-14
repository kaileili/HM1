from operator import add
from pyspark.shell import sqlContext
import sys
from pyspark import SparkContext

if __name__ == "__main__":
	# input_file_1_path = "country.csv"
	# input_file_2_path = "countrylanguage.csv"
	# output_file_path = "output.txt"
	input_file_1_path = sys.argv[1】
	input_file_2_path = sys.argv[2]
	output_file_path = sys.argv[3]
	sc = SparkContext.getOrCreate()

	countryname = sc.textFile(input_file_1_path)
	countrylanguage = sc.textFile(input_file_2_path)
	result = countryname.map(lambda row: row.split(',')[0]).distinct() \ 
	.subtract(countrylanguage.map(lambda row: row.split(',')[0]).distinct()) \
		.map(lambda row: str(row) + "\n").collect()

	#
	output_file = open(output_file_path, "w+")
	output_file.writelines("\n")
	output_file.writelines(result)
	output_file.close()
