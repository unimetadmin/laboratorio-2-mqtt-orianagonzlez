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
        df = pd.read_csv('datos_nevera.csv')

        tiempo = datetime.datetime.strptime(df.iloc[-1, -1], '%Y-%m-%d %H:%M:%S')
        tiempo += datetime.timedelta(minutes=5)

    except:
        tiempo = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    mean_temp = 10
    std_temp = 2
    min_hielo = 0
    max_hielo = 10

    mensaje_hielo = False
    primera_vez = True

    while True:

        #Cada 5 min
        temp_nev = round(np.random.normal(mean_temp, std_temp), 2)
        payload = { "temperatura": temp_nev, "capacidad": "", "tiempo": str(tiempo) }
        
        #cada 10 min
        if (mensaje_hielo or primera_vez):
            capacidad_hielo = round(np.random.uniform(min_hielo, max_hielo), 2)
            payload["capacidad"]= capacidad_hielo
           
            mensaje_hielo = False
        else:
            mensaje_hielo = True
        
        primera_vez = False

        print("\nPublicando mensaje")
        client.publish('casa/cocina/temperatura_nevera',json.dumps(payload),qos=0)
        print(payload)

        time.sleep(1)

        #Se suman los 5 minutos al tiempo
        tiempo += datetime.timedelta(minutes=5)
        
if __name__ == '__main__':
	main()
	sys.exit(0)
