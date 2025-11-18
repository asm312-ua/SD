import socket
import threading
import json
import time
import os
import pprint
from confluent_kafka import Producer, Consumer, KafkaError

# ============================================================
# Configuración global
# ============================================================
CENTRAL_HOST = 'localhost'
CENTRAL_PORT_ESTADOS = 6000       # Monitor -> Central (estados CP)
CENTRAL_PORT_SOLICITUDES = 6001   # (Si se usase sockets para solicitudes CPs)
SOCKET_BUFFER = 8192

# Kafka topics (confirmados)
KAFKA_BROKER = 'localhost:9092'
TOPIC_SOLICIT_DRIVER = 'solicitudes_driver'   # drivers -> central (nueva)
TOPIC_SOLICIT_CP = 'peticiones_carga'         # (posible topic legacy / CP-related)
TOPIC_SOLICIT_ENGINE = 'peticiones_engine'
TOPIC_TICKETS = 'tickets_cp'                  # tickets finales que envían los CPs
TOPIC_RESPUESTAS = 'respuestas_central'      # central -> drivers (respuesta unificada)
# nota: si usabamos 'respuestas_driver' antes, ahora usamos 'respuestas_central' por coherencia

# Estado global compartido
estados_cp = {}                    # dict cp_id -> info
lock_estados = threading.Lock()
pp = pprint.PrettyPrinter(indent=4)


# ============================================================
# Manejo de la base de datos
# ============================================================
FICHERO_BASE_DATOS = "basedatos.json"

def cargar_cps_basedatos():
    if not os.path.exists(FICHERO_BASE_DATOS):
        print("[Central] No se ha encontrado una base de datos.")
    with open(FICHERO_BASE_DATOS, "r") as f:
        datos = json.load(f)
    print(f"[Central] Base de datos cargada ({len(datos)} CPs).")
    return datos

def guardar_cps_basedatos(data):
    with open(FICHERO_BASE_DATOS, "w") as f:
        json.dump(data, f, indent=2)

# ============================================================
# KAFKA: inicialización
# ============================================================
def inicializar_kafka():
    try:
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': 'central-unificada-group',
            'auto.offset.reset': 'earliest'
        })
        # Subscribir a los topics que nos interesan: peticiones (legacy), tickets, y solicitudes de drivers
        consumer.subscribe([TOPIC_SOLICIT_CP, TOPIC_TICKETS, TOPIC_SOLICIT_DRIVER, TOPIC_SOLICIT_ENGINE])
        print("[Central] Kafka inicializado (producer + consumer).")
        return producer, consumer
    except Exception as e:
        print(f"[Central] Aviso: Kafka no disponible: {e}")
        return None, None


# ============================================================
# Gestor de estados de CP (desde monitor por sockets)
# ============================================================
def manejar_estado_cp(conn, addr):
    buffer = ''
    with conn:
        while True:
            data = conn.recv(SOCKET_BUFFER)
            if not data:
                break
            buffer += data.decode()
            while '\n' in buffer:
                mensaje, buffer = buffer.split('\n', 1)
                try:
                    state = json.loads(mensaje)
                    cp_id = state.get('cp_id')
                    if not cp_id:
                        continue
                    with lock_estados:
                        # Si NO existe el CP lo cremos con los datos por defecto
                        if cp_id not in estados_cp:
                            print(f"\n[Central] Registrado un nuevo CP: {cp_id}")
                            estados_cp[cp_id] = {
                                "ubicacion": "Desconocida",
                                "precio_kwh": 0.3,
                                "estado": "DESCONECTADO",
                                "healthy": False,
                                "in_use": False,
                            }

                        # Si SÍ existe, actualizamos sólo los campos dinámicos
                        estados_cp[cp_id]['estado'] = "ACTIVO" if state.get('healthy', False) else "DESCONECTADO"
                        estados_cp[cp_id]['healthy'] = state.get('healthy', False)
                        estados_cp[cp_id]['in_use'] = state.get('in_use', False)
                        estados_cp[cp_id]['ip'] = state.get('ip', addr[0])
                        estados_cp[cp_id]['cmd_port'] = state.get('cmd_port')
                        guardar_cps_basedatos(estados_cp)
                except Exception as e:
                    print(f"[Central] Error procesando estado: {e}")


def servidor_estados_cp():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((CENTRAL_HOST, CENTRAL_PORT_ESTADOS))
        s.listen()
        print(f"[Central] Escuchando estados en puerto {CENTRAL_PORT_ESTADOS}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=manejar_estado_cp, args=(conn, addr), daemon=True).start()


# ============================================================
# Enviar órdenes a un CP (por socket)
# ============================================================
def enviar_orden(cp_id, action):
    with lock_estados:
        info = estados_cp.get(cp_id)
    if not info:
        print(f"[Central] No se conoce el CP {cp_id}")
        return

    try:
        with socket.create_connection((info['ip'], info['cmd_port']), timeout=3) as s:
            s.sendall(json.dumps({'cp_id': cp_id, 'action': action}).encode('utf-8'))
            resp = s.recv(1024)
            print(f"[Central] Respuesta de {cp_id}: {resp.decode('utf-8')}")
    except Exception as e:
        print(f"[Central] Error al enviar orden a {cp_id}: {e}")


# ============================================================
# Menú
# ============================================================
def menu_central():
    while True:
        time.sleep(1)
        print("\n======================== MENU CENTRAL ===========================")
        print("1. Ver estado de CPs")
        print("2. Activar CP")
        print("3. Apagar CP")
        print("4. Registrar/Manejar datos de CPs")
        print("5. Salir")
        print("=================================================================")
        op = input("> ").strip()
        if op == '1':
            with lock_estados:
                print("\nID\tUbicación\t\tPrecio\t\tEstado\t\tHealthy\tIn_Use")
                print("----------------------------------------------------------------------------------")
                for cp, info in estados_cp.items():
                    print(f"{cp}\t{info['ubicacion'][:18]:<18}\t{info['precio_kwh']} €/kWh\t{info['estado']}\t{info['healthy']}\t{info['in_use']}")
        elif op == '2':
            enviar_orden(input("CP ID: ").strip(), 'activate')
        elif op == '3':
            enviar_orden(input("CP ID: ").strip(), 'sleep')
        elif op == '4':
            cp_id = input("CP ID: ")
            info = estados_cp.get(cp_id)
            print(f"{cp_id} -> Ubicación actual: {info['ubicacion']} | Precio actual: {info['precio_kwh']}")
            opcion = input("¿Desea cambiar los datos? (S/N): ")
            if opcion == 'S':
                nueva_ubicacion = input(f"Introduce la ubicación de {cp_id}: ")
                nuevo_precio = input(f"Introduce el precio/kWh de {cp_id}: ")
                if nueva_ubicacion is None:
                    nueva_ubicacion = "Desconocida"
                if nuevo_precio is None:
                    nuevo_precio = 0.3
                estados_cp[cp_id]['ubicacion'] = nueva_ubicacion
                estados_cp[cp_id]['precio_kwh'] = nuevo_precio
            elif opcion == 'N':
                continue
            else:
                print("Opción inválida.")
        elif op == '5':
            break
        else:
            print("Opción inválida.")


# ============================================================
# KAFKA: funciones auxiliares (producción de respuestas)
# ============================================================
def enviar_respuesta_kafka(producer, driver_id, cp_id, estado, status, precio_kwh=None):
    if producer is None:
        print(f"[KAFKA:{TOPIC_RESPUESTAS}] fallback -> driver={driver_id} cp={cp_id} estado={estado} status={status}")
        return

    payload = {
        'driver_id': driver_id,
        'cp_id': cp_id,
        'estado': estado,
        'mensaje': estado,
        'status': status
    }
    if precio_kwh is not None:
        payload['precio_kwh'] = precio_kwh
    try:
        producer.produce(TOPIC_RESPUESTAS, key=driver_id, value=json.dumps(payload).encode('utf-8'))
        producer.flush(3)
    except Exception as e:
        print(f"[Central] Error al producir respuesta Kafka: {e}")



# ============================================================
# KAFKA: procesado de mensajes entrantes (unificado)
# ============================================================
def procesar_mensaje_kafka(producer, topic, data):
    try:
        if topic in (TOPIC_SOLICIT_CP, TOPIC_SOLICIT_DRIVER, TOPIC_SOLICIT_ENGINE):
            driver_id = data.get('driver_id')
            cp_id = data.get('cp_id')
            print(f"[Central][KAFKA] Solicitud recibida: driver={driver_id} cp={cp_id} (topic={topic})")

            with lock_estados:
                estado_cp = estados_cp.get(cp_id)
                precio_kwh = estado_cp.get('precio_kwh', 0.30) if estado_cp else 0.30

            # Validaciones de estado
            if not estado_cp:
                mensaje = f"CP {cp_id} desconocido"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                print(f"[Central] CP {cp_id} desconocido.")
                return

            if not estado_cp.get('healthy', False):
                mensaje = f"CP {cp_id} no saludable"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                print(f"[Central] Denegada carga: CP {cp_id} no saludable.")
                return

            if estado_cp.get('in_use', False):
                mensaje = f"CP {cp_id} ocupado"
                enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje, 'ko')
                print(f"[Central] Denegada carga: CP {cp_id} en uso.")
                return

            # Si está libre y saludable -> autorizar carga
            mensaje_ok = f"Carga autorizada en {cp_id}"
            enviar_respuesta_kafka(producer, driver_id, cp_id, mensaje_ok, 'ok', precio_kwh)
            print(f"[Central] Autorizada carga para {driver_id} en {cp_id} con precio: {precio_kwh}")

            # Notificar al Engine también para que empiece a cargar
            # (usa el mismo topic de respuestas)
            payload_engine = {
                'driver_id': driver_id,
                'cp_id': cp_id,
                'estado': 'start',
                'mensaje': 'Iniciar carga',
                'status': 'ok',
                'precio_kwh': precio_kwh
            }
            try:
                producer.produce(TOPIC_RESPUESTAS, key=cp_id, value=json.dumps(payload_engine).encode('utf-8'))
                producer.flush(3)
                print(f"[Central] Notificación enviada al Engine {cp_id} para iniciar carga.")
            except Exception as e:
                print(f"[Central] Error al notificar al Engine: {e}")

            # Actualizar estado local como "en uso"
            with lock_estados:
                estados_cp[cp_id]['in_use'] = True

        elif topic == TOPIC_TICKETS:
            cp_id = data.get('cp_id')
            driver_id = data.get('driver_id', 'unknown')
            kwh = data.get('kwh')
            cost = data.get('cost')

            print(f"[Central] Ticket final recibido de {cp_id}: kWh={kwh} cost={cost} driver={driver_id}")

            # Guardar ticket y liberar CP
            with lock_estados:
                rec = estados_cp.setdefault(cp_id, {})
                rec['last_ticket'] = data
                rec['in_use'] = False

            # Reenviar ticket al driver
            payload_ticket = {
                'driver_id': driver_id,
                'cp_id': cp_id,
                'estado': 'ticket_final',
                'mensaje': f'Carga finalizada en {cp_id}',
                'status': 'ok',
                'kwh': kwh,
                'cost': cost
            }

            try:
                producer.produce(TOPIC_RESPUESTAS, key=driver_id, value=json.dumps(payload_ticket).encode('utf-8'))
                producer.flush(3)
                print(f"[Central] Ticket reenviado al driver {driver_id}.")
            except Exception as e:
                print(f"[Central] Error al reenviar ticket al driver: {e}")

        else:
            print(f"[Central] Mensaje en topic desconocido {topic}: {data}")

    except Exception as e:
        print("[Central] Error procesando mensaje Kafka:", e)



def kafka_worker(producer, consumer):
    if consumer is None:
        return
    print("[Central] kafka_worker activo: escuchando topics:", TOPIC_SOLICIT_CP, TOPIC_TICKETS, TOPIC_SOLICIT_DRIVER)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("[Central] Kafka error:", msg.error())
                continue
            try:
                topic = msg.topic()
                data = json.loads(msg.value().decode('utf-8'))
                procesar_mensaje_kafka(producer, topic, data)
            except Exception as e:
                print("[Central] Error procesando mensaje Kafka:", e)
    except Exception as e:
        print("[Central] kafka_worker terminado:", e)


# ============================================================
# Ejecución
# ============================================================
def main():
    global estados_cp
    estados_cp = cargar_cps_basedatos()

    producer, consumer = inicializar_kafka()
    # Si hay consumer, arrancamos el worker de Kafka
    if consumer is not None:
        threading.Thread(target=kafka_worker, args=(producer, consumer), daemon=True).start()

    # Servidor sockets para recibir estados del monitor de CPs
    threading.Thread(target=servidor_estados_cp, daemon=True).start()

    print("[Central] Servidor iniciado.")
    menu_central()

if __name__ == '__main__':
    main()
