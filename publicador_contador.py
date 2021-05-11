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
        df = pd.read_csv('datos_contador.csv')

        tiempo = datetime.datetime.strptime(df.iloc[-1, -1], '%Y-%m-%d %H:%M:%S')
        tiempo += datetime.timedelta(minutes=1)

    except:
        tiempo = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    min_personas = 0
    max_personas = 10

    while True:

        #cada minuto
        personas = int(np.random.uniform(min_personas, max_personas))
        payload = { "cantidad": personas, "mensaje": "", "tiempo": str(tiempo) }
       
        if (personas > 5):
            payload["mensaje"] = "Alerta! Numero de personas excedido"

        print("\nPublicando mensaje")
        client.publish('casa/sala/contador_personas',json.dumps(payload),qos=0)
        print(payload)

        time.sleep(1)
        #Se suma 1 minuto al tiempo
        tiempo += datetime.timedelta(minutes=1)
        
if __name__ == '__main__':
	main()
	sys.exit(0)
