from datetime import datetime, date, time, timedelta
import sys
import json
import os
import ast
from flask import Flask

# importing pyspark
import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Basics').getOrCreate()

# import pyspark class Row from module sql
from pyspark.sql import *
from pyspark.sql.types import *
import tempfile

# ml
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel

# start a kafka consumer session
from kafka.consumer import KafkaConsumer
consumer = KafkaConsumer("titanic", bootstrap_servers=['ip-172-31-12-218.us-east-2.compute.internal:6667'])
print('consumer launched')

testSchema = ["PassengerId",
    "Pclass",
    "Name",
    "Sex",
    "Age",
    "SibSp",
    "Parch",
    "Ticket",
    "Fare",
    "Cabin",
    "Embarked"
]

pipeline = Pipeline.load("/home/ubuntu/titanic/pipeline")
model = PipelineModel.load("/home/ubuntu/titanic/model")



def getTrain(msg):
    # put passenger info into dataframe
    # print msg
    # combine two lists into list of tuple
    # combined = map(lambda x, y: (x, y), trainSchema, msg)
    msg = [ast.literal_eval(msg)]
    msg[0][0] = float(msg[0][0])
    msg[0][1] = float(msg[0][1])
    msg[0][4] = float(msg[0][4])
    msg[0][5] = float(msg[0][5])
    msg[0][6] = float(msg[0][6])
    msg[0][8] = float(msg[0][8])
    df = spark.createDataFrame(msg, testSchema)
    result = model.transform(df)
    result.show()
    return result

def main():
    message = consumer.next()
    info = message.value
    result = getTrain(info)

    # for message in consumer:
    #     info = message.value
    #     DF = getTrain(info)


if __name__ == '__main__':
    main()


