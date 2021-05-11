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

#Puede que haya un error de permiso si se abre el cvs mientras esta corriendo el suscriptor y publicador

def on_connect(client, userdata, flags, rc):
	print('Publicador conectado')

def main():
    client = paho.mqtt.client.Client("Casa Oriana", False)
    client.qos = 0
    client.connect(host='localhost')

    capacidad = 100
    nivel = 100
    t = 0
    
    #En caso de haber uno, buscamos el ultimo tiempo registrado
    try:
        df = pd.read_csv('datos_tanque.csv')

        tiempo = datetime.datetime.strptime(df.iloc[-1, -1], '%Y-%m-%d %H:%M:%S')

    except:
        tiempo = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)

    mean_bajada = capacidad * 0.10
    std_bajada = capacidad * 0.05

    mean_subida = capacidad * 0.20
    std_subida = capacidad * 0.05

    while True:
        if (t != 0):
            #cada 10 minutos el tanque baja
            
            decremento = np.random.normal(mean_bajada, std_bajada)
            nivel -= decremento
            if (nivel < 0):
                nivel = 0


            #cada 30 minutos el tanque sube
            if (t == 3):
                t = 0
                incremento = np.random.normal(mean_subida, std_subida)
                nivel += incremento
                if (nivel > 100):
                    nivel = 100

            payload = { "nivel": round(nivel, 2), "mensaje": "", "tiempo": str(tiempo)}

            if (nivel == 0):
                mensaje2 = "Alerta! Su tanque esta vacio"
                payload["mensaje"] = mensaje2 
            elif (nivel <= 50):
                mensaje1 = "Alerta! Su tanque esta por la mitad o menos de su capacidad"
                payload["mensaje"] = mensaje1      
               
            print("\nPublicando mensaje")
            client.publish('casa/baño/nivel_tanque',json.dumps(payload),qos=0)
            print(payload)
        
        t += 1
        time.sleep(2)
        #Sumandole 10 minutos al tiempo
        tiempo += datetime.timedelta(minutes=10)
        
if __name__ == '__main__':
	main()
	sys.exit(0)
