import sys
import paho.mqtt.client
import pandas as pd
import json
import datetime
import psycopg2

connection = psycopg2.connect(user="vitcowpc",
                            password="sN6DIHHJrsarNdtIWhrLZkjItas306Z6",
                            host="queenie.db.elephantsql.com",
                            database="vitcowpc"
                            )

def on_connect(client, userdata, flags, rc):
	print('connected (%s)' % client._client_id)
	client.subscribe(topic='casa/#', qos=2)

def insert(query, *args):
	cursor = connection.cursor()
	cursor.execute(query, args)
	connection.commit()

def on_message(client, userdata, message):
	print('------------------------------')
	print('topic: %s' % message.topic)
	print('payload: %s' % message.payload)
	print('qos: %d' % message.qos)

	data_original = json.loads(message.payload)

	data_original['tiempo'] = datetime.datetime.strptime(data_original['tiempo'], '%Y-%m-%d %H:%M:%S')
	
	df = pd.DataFrame([data_original])

	if (message.topic == "casa/cocina/temperatura_nevera"):
		
		df.to_csv('datos_nevera.csv', mode='a', header=False)

		q = '''
			INSERT INTO nevera (temperatura, tiempo)
			values (%s, %s)
			'''
		
		insert(q, data_original["temperatura"], data_original["tiempo"])

	elif (message.topic == "casa/sala/contador_personas"):
		df.to_csv('datos_contador.csv', mode='a', header=False)

		if (data_original["mensaje"] != ''):

			q = '''
				INSERT INTO contador (cantidad, mensaje, tiempo)
				values (%s, %s, %s)
				'''
			insert(q, data_original["cantidad"], data_original["mensaje"], data_original["tiempo"])
		else:
			q = '''
				INSERT INTO contador (cantidad, tiempo)
				values (%s, %s)
				'''
			insert(q, data_original["cantidad"], data_original["tiempo"])

	elif (message.topic == "casa/cocina/temperatura_olla"):
		df.to_csv('datos_olla.csv', mode='a', header=False)

		if (data_original["mensaje"] != ''):

			q = '''
				INSERT INTO olla (temperatura, mensaje, tiempo)
				values (%s, %s, %s)
				'''
			insert(q, data_original["temperatura"], data_original["mensaje"], data_original["tiempo"])
		else:
			q = '''
				INSERT INTO olla (temperatura, tiempo)
				values (%s, %s)
				'''
			insert(q, data_original["temperatura"], data_original["tiempo"])

	elif (message.topic == "casa/sala/alexa_echo"):
		df.to_csv('datos_alexa.csv', mode='a', header=False)

		q = '''
			INSERT INTO alexa (temperatura, tiempo)
			values (%s, %s)
			'''
		insert(q, data_original["temperatura"], data_original["tiempo"])

	elif (message.topic == "casa/baño/nivel_tanque"):
		df.to_csv('datos_tanque.csv', mode='a', header=False)

		if (data_original["mensaje"] != ''):

			q = '''
				INSERT INTO tanque (nivel, mensaje, tiempo)
				values (%s, %s, %s)
				'''
			insert(q, data_original["nivel"], data_original["mensaje"], data_original["tiempo"])
		else:
			q = '''
				INSERT INTO tanque (nivel, tiempo)
				values (%s, %s)
				'''
			insert(q, data_original["nivel"], data_original["tiempo"])

	else:
		print('No se identifico la direccion')

def main():
	client = paho.mqtt.client.Client(client_id='oriana', clean_session=False)
	client.on_connect = on_connect
	client.on_message = on_message
	client.connect(host='127.0.0.1', port=1883)
	client.loop_forever()

if __name__ == '__main__':
	main()

sys.exit(0)
