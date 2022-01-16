import serial
import time
import csv
import pika
from datetime import datetime
import json

def writeTemp(row):
   with open("temp_data.csv","a") as f:
            writer = csv.writer(f,delimiter=",")
            writer.writerow(row)

def publishTemp(temp):
   message=json.dumps(temp)
   channel.queue_declare(queue='temp', durable=True)
   channel.exchange_declare(exchange='temp', exchange_type='topic', durable=True)
   channel.basic_publish(exchange='temp',
                            routing_key='temp',
                            body=message,
                            properties=pika.BasicProperties(
                                        content_type="application/json",
                                        headers={'__TypeId__': 'com.example.SensorService.domain.dto.TempDto'},
                                        content_encoding= 'UTF-8',
                                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
                            ))

with serial.Serial('/dev/ttyACM0', 9600, timeout=300) as ser:
   while True:
      line = ser.readline()
      now = datetime.now()
      strd = now.strftime("%d-%b-%Y %H:%M:%S")
      strd2 = now.strftime("%Y-%m-%d %H:%M:%S")
      x = line.decode("utf-8").split(":")
      t2=round(float(x[1][:-1])-3.0,2)
      t1=round(float(x[5][:-1])-4.0,2)
      t3=round(float(x[9][:-1])-1.0,2)

      writeTemp([strd,str(t1)+"C",str(t2)+"C",str(t3)+"C"])

      connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
      channel = connection.channel()

      publishTemp({'value': t1, 'dateOfMeasure': strd2, 'tempSensorId':1, 'tempState': 'CREATED'})
      publishTemp({'value': t2, 'dateOfMeasure': strd2, 'tempSensorId':2, 'tempState': 'CREATED'})
      publishTemp({'value': t3, 'dateOfMeasure': strd2, 'tempSensorId':3, 'tempState': 'CREATED'})

      connection.close()

