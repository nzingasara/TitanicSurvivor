import time
import sys
import json
import os
import csv
import json
from kafka.producer import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['ip-172-31-12-218.us-east-2.compute.internal:6667'])
print('producer initiated')

def sendData():
    with open('test.csv') as myfile:
        read_csv = csv.reader(myfile, delimiter=',')
        line = 0
        for row in read_csv:
            print type(row)
            producer.send("titanic", json.dumps(row))
            print(row)
            line += 1
            time.sleep(1)

        print('Processed ' + str(line) + ' lines')

def main():
    sendData();

if __name__ == '__main__':
    main()


