from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from json import dumps, loads
from time import sleep
import sys
import os
from threading import Thread

# ============================================================
# Configuración topics
# ============================================================
TOPIC_SOLICITUDES_DRIVER = "solicitudes_driver"  # Driver produce
TOPIC_RESPUESTAS_CENTRAL = "respuestas_central"  # Driver escucha
barrier = True

# ============================================================
# Callback de envío
# ============================================================
def confirmacion_envio(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(f"[Callback] Mensaje enviado a '{msg.topic()}'")


# ============================================================
# Hilo que escucha respuestas de la central
# ============================================================
def escuchar_respuestas(broker, driver_id):
    consumer_config = {
        'bootstrap.servers': broker,
        'group.id': f'driver-{driver_id}',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC_RESPUESTAS_CENTRAL])
    global barrier
    print(f"[{driver_id}] Escuchando respuestas en '{TOPIC_RESPUESTAS_CENTRAL}'...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
                continue

            data = loads(msg.value().decode('utf-8'))

            # Filtrar solo las respuestas para este driver
            if data.get('driver_id', '').lower() != driver_id.lower():
                continue

            estado = data.get('estado', '').lower()
            cp_id = data.get('cp_id', 'unknown')

            # 🔸 Caso especial: ticket final recibido
            if estado == 'ticket_final':
                kwh = float(data.get('kwh', 0))
                coste = float(data.get('cost', 0))
                precio_kwh = float(data.get('precio_kwh',0))

                print("\n============= TICKET FINAL DE CARGA =============")
                print(f"Punto de carga: {cp_id}")
                print(f"Energía suministrada: {kwh:.2f} kWh")
                print(f"Precio por kWh: {precio_kwh:.3f} €/kWh")
                print(f"IMPORTE TOTAL: {coste:.2f} €")
                print("==================================================\n")
                barrier = False
                #print(f"[{driver_id}] Finalizando recepción de mensajes.")
                #consumer.close()
                #print(f"[{driver_id}] Cerrando driver...")
                #return 0

            # 🔹 Otros mensajes de la central
            else:
                print(f"[RESPUESTA] CP={cp_id} -> {data.get('mensaje', estado)}")

    except KeyboardInterrupt:
        print(f"\n[DRIVER {driver_id}] Finalizando recepción de mensajes...")
    finally:
        consumer.close()


# ============================================================
# Manejo de solicitudes
# ============================================================
def manejo_solicitudes(producer, driver_id, fichero=None):
    global barrier
    # HAY FICHERO
    if fichero:
        if not os.path.exists(fichero):
            print(f"Error: el fichero '{fichero}' no existe.")
            sys.exit(1)

        with open(fichero, 'r') as f:
            cp_list = [line.strip() for line in f if line.strip()]

        for cp_id in cp_list:
            enviar_solicitud(producer, driver_id, cp_id)
            while barrier:
                sleep(1)
            barrier = True
        print(f"[{driver_id}] Se han enviado correctamente todas las solicitudes.")
    
    # NO HAY FICHERO
    else:
        cp_id = input(f"[{driver_id}] Introduce el ID del punto de recarga: ")
        enviar_solicitud(producer, driver_id, cp_id)

    producer.flush()


# ============================================================
# Envío de una solicitud
# ============================================================
def enviar_solicitud(producer, driver_id, cp_id):
    mensaje = {'driver_id': driver_id, 'cp_id': cp_id}
    producer.produce(TOPIC_SOLICITUDES_DRIVER, value=dumps(mensaje), callback=confirmacion_envio)
    producer.poll(0)
    print(f"[{driver_id}] Solicitud enviada a CP: {cp_id}")


# ============================================================
# Inicialización del driver
# ============================================================
def iniciar_driver(broker, driver_id, fichero=None):
    print(f"[{driver_id}] EV_Driver iniciado.")
    print(f"[{driver_id}] Conectado al broker Kafka en {broker}")

    producer = Producer({'bootstrap.servers': broker})

    # Lanzar hilo para escuchar respuestas
    hilo_respuestas = Thread(target=escuchar_respuestas, args=(broker, driver_id))
    hilo_respuestas.daemon = True
    hilo_respuestas.start()

    # Enviar solicitudes
    manejo_solicitudes(producer, driver_id, fichero)

    hilo_respuestas.join()


# ============================================================
# Ejecución
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Uso: python EV_Driver.py <broker> <driver_id> [fichero]")
        sys.exit(1)

    broker = sys.argv[1]
    driver_id = sys.argv[2]
    fichero = None
    
    if len(sys.argv) == 4:
        fichero = sys.argv[3]

    iniciar_driver(broker, driver_id, fichero)