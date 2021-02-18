from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("AutoPostSales")
sc = SparkContext(conf = conf)

raw_rdd = sc.textFile("data.csv")

def extract_vin_key_value(line):
    
    line_split = line.split(",")
    
    vin = line_split[2]
    make = line_split[3]
    year = line_split[5]
    incident_type = line_split[1]
    
    return ((vin),(make,year,incident_type))

vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

def populate_make(value_list):
    
    sorted(value_list)
    
    output_value_list = []
            
    for val in value_list:
        if val[0].strip() != '':
            make = val[0]
        
        if val[1].strip() != '':
            year = val[1]
            
        output_value_list.append((make,year,val[2]))
        
    return output_value_list

enhance_make = enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

def extract_make_key_value(data):
    
    if data[2] == "A":
        return (data[0],1)
    else:
        return(data[0],0)
    
make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

enhance_make = make_kv.reduceByKey(lambda x,y:x+y)

final_rdd = enhance_make.collect()

with open('output.txt', 'w') as filehandle:
    for listitem in final_rdd:
        filehandle.write(listitem[0] + "," + str(listitem[1]) + "\n")

sc.stop()
