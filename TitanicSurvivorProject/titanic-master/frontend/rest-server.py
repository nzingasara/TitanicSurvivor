#!flask/bin/python
from flask import Flask, jsonify, abort, request, make_response, url_for
from flask import render_template, redirect

# kafka consumer
from datetime import datetime, date, timedelta
import sys
import json
import os
import ast
import time

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
from kafka.producer import KafkaProducer
consumer = KafkaConsumer("titanic", bootstrap_servers=['ip-172-31-12-218.us-east-2.compute.internal:6667'])
producer = KafkaProducer(bootstrap_servers=['ip-172-31-12-218.us-east-2.compute.internal:6667'])

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
    return result

app = Flask(__name__, static_url_path="")

@app.route('/', methods=['GET'])
def home_page():
	try:
		info = consumer.next().value
		print info
		result = getTrain(info)
		result.show()
		row = result.collect()[0].asDict()
		pid = int(row.get('PassengerId'))
		pclass = int(row.get('Pclass'))
		name = row.get('Name')
		sex = row.get('Sex')
		age = int(row.get('Age'))
		sibsp = int(row.get('SibSp'))
		parch = int(row.get('Parch'))
		ticket = row.get('Ticket')
		fare = row.get('Fare')
		cabin = row.get('Cabin')
		embarked = row.get('Embarked')
		if embarked == "C":
			embarked = "Cherbourg"
		elif embarked == "Q":
			embarked = "Queenstown"
		else: 
			embarked = "Southampton"

		prediction = 'Survived' if row.get('prediction') == 1.0 else 'Deceased'
		print embarked
		return render_template('index.html', pid=pid, pclass=pclass, name=name, sex=sex, age=age,
			sibsp=sibsp, parch=parch, ticket=ticket, fare=fare, cabin=cabin, embarked=embarked, prediction=prediction)
	except:
		return render_template('index.html', ssss=1)

@app.route('/data', methods=['GET', 'POST'])
def student_add_page():
    if request.method == 'POST':   
    	# kafka push code
    	data = [request.form['pid'].encode('ascii','ignore'), request.form['pclass'].encode('ascii','ignore'), request.form['name'].encode('ascii','ignore'),
    	request.form['sex'].encode('ascii','ignore'), request.form['age'].encode('ascii','ignore'), request.form['sibsp'].encode('ascii','ignore'),
    	request.form['parch'].encode('ascii','ignore'), request.form['ticket'].encode('ascii','ignore'), request.form['fare'].encode('ascii','ignore'), 
    	request.form['cabin'].encode('ascii','ignore'), request.form['embarked'].encode('ascii','ignore')]

    	# data = [request.form['pid'], request.form['pclass'], request.form['name'],
    	# request.form['sex'], request.form['age'], request.form['sibsp'],
    	# request.form['parch'], request.form['ticket'], request.form['fare'], 
    	# request.form['cabin'], request.form['embarked']]
    	print data
    	producer.send("titanic", json.dumps(data))
    	producer.flush()
        return render_template('data.html')
    else:
        return render_template('data.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)