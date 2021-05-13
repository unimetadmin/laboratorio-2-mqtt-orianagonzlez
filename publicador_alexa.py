import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
import datetime
import requests
import pandas as pd


def on_connect(client, userdata, flags, rc):
	print('Publicador conectado')

def main():
    client = paho.mqtt.client.Client("Casa Oriana", False)
    client.qos = 0
    client.connect(host='localhost')

    url = "https://api.ambeedata.com/weather/latest/by-lat-lng"
    querystring = {"lat":"10.491","lng":"-66.902 "}
    headers = {
        'x-api-key': "WESQUqHcevN50Jdjoe3a5Q5BDvubRi64AyIpygl1",
        'Content-type': "application/json"
        }
    
    response = requests.request("GET", url, headers=headers, params=querystring)
    temp = int(json.loads(response.text)["data"]["temperature"])

    #En caso de haber uno, buscamos el ultimo tiempo registrado
    try:
        df = pd.read_csv('datos_alexa.csv')

        tiempo = datetime.datetime.strptime(df.iloc[-1, -1], '%Y-%m-%d %H:%M:%S')
        
        tiempo += datetime.timedelta(minutes=5)

    except:
        tiempo = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)


    while True:
        
        payload = { "temperatura": temp, "tiempo": str(tiempo) }
        print("\nPublicando mensaje")
        client.publish('casa/sala/alexa_echo',json.dumps(payload),qos=0)
        print(payload)

        
        time.sleep(3)
        #Sumando 10 minutos al tiempo
        tiempo += datetime.timedelta(minutes=10)
        temp -= 1

        
if __name__ == '__main__':
	main()
	sys.exit(0)