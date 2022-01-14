from pyspark import SparkConf, SparkContext
import sys
from operator import add

if __name__ == "__main__":
    sc =SparkContext.getOrCreate()
    contient = data
    if x[1] in ["China" ,"India","Korea", "Japan", "Indonesia","Pakistan","Bangladesh","Philippines","Vietnam","Turkey"]:
        d=x[0].split('/')
        dd=""
        dd+=d[0]+'/'+d[2]
        dd=dd.split()
    sc =SparkContext.getOrCreate()
    
        
        return (str(dd[0]),int(x[2]))
        
    
    
data=sc.textFile("Country.csv") \
.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[1],line[2],line[5])).collect()
sc.parallelize(data).map(date).filter(lambda x: x is not None).reduceByKey(add).filter(lambda (x,y):y>=5000000).collect()