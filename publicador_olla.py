import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
import datetime
import pandas as pd

def on_connect(client, userdata, flags, rc):
	print('Publicador conectado')

def main():
    client = paho.mqtt.client.Client("Casa Oriana", False)
    client.qos = 0
    client.connect(host='localhost')

    #En caso de haber uno, buscamos el ultimo tiempo registrado
    try:
        df = pd.read_csv('datos_olla.csv')

        tiempo = datetime.datetime.strptime(df.iloc[-1, -1], '%Y-%m-%d %H:%M:%S')
        tiempo += datetime.timedelta(seconds=1)

    except:
        tiempo = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    min_temp = 0
    max_temp = 150
    usando = True

    while usando:

        #Cada segundo
        temp = round(np.random.uniform(min_temp, max_temp), 2)
        payload = { "temperatura": temp, "mensaje": "", "tiempo": str(tiempo) }
        

        if (temp >= 100):
            payload["mensaje"] = "El agua de la olla ya hirvio"

        print("\nPublicando mensaje")
        client.publish('casa/cocina/temperatura_olla',json.dumps(payload),qos=0)
        print(payload)

        time.sleep(1)
        #Se suma 1 segundo al tiempo
        tiempo += datetime.timedelta(seconds=1)

        
if __name__ == '__main__':
	main()
	sys.exit(0)
