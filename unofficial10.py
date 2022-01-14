from pyspark import SparkConf, SparkContext
import sys
from operator import add

def cor(x):
    d=x[0].split('/')
    dd=""
    dd+=d[0]+'/'+d[2]
    dd=dd.split()
    dd=str(dd[0])

    dd+=' '+ x[1]+' '+x[2]
    dd=str(dd)
    return (dd,int(x[3]))

Name=sc.textFile("Country.csv") \
.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[1],line[3],line[4],line[5])).collect()

Lan=sc.textFile("Countrylanguage.csv") \
.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[1],line[3],line[4],line[5])).collect()


ds1=sc.parallelize(Name).map(cor).filter(lambda x: x is not None).reduceByKey(add).collect()
ds2=sc.parallelize(Lan).map(cor).filter(lambda x: x is not None).reduceByKey(add).collect()
ds1=sc.parallelize(ds1)
ds2=sc.parallelize(ds2)
ds1.join(ds2).collect()