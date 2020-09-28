### SPARK-LOCAL 
### READ FROM JUPYTER NOTEBOOK USE HDFS/CLOUDERA-LOCAL/MYPC_LOCAL

###########################################################


########### READING FROM CLOUDERA(EXTERNAL SERVER) #########
## STEP 1. search for the hostname ip inside your cloudera

hostname -i

## STEP 2. ping in your windows command prompt to create connectivity

ping 192.168.247.136

## when successful it means connectivity has been created

###########  END ###########################################


########### READING FROM MY LAPTOP LOCAL #################

df=spark.read.option("inferSchema","true").csv(r"C:\Users\konad\Desktop\data-master\retail_db\departments\part-*")
df.printSchema

###########      END      #####################################

###########   READING FROM CLOUDERA HDFS  ####################

df=spark.read.options(inferSchema="true").csv(r"hdfs://192.168.247.136//user/cloudera/test1/sales_records")
df.printSchema
df.show()

##########   END		######################################


########## READING FROM CLOUDERA LOCAL #######################

