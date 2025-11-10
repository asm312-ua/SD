import socket
import json
import sys
import os
import time
import threading
import signal
from confluent_kafka import Producer, Consumer, KafkaError
from typing import Tuple

# ============================================================
# Soporte multiplataforma (detección de Windows para input no bloqueante)
# ============================================================
try:
    import msvcrt
    _IS_WINDOWS = True
except Exception:
    import select
    _IS_WINDOWS = False

# ============================================================
# Validación de parámetros de entrada
# ============================================================
if len(sys.argv) != 5:
    print("Uso: python EV_CP_E.py <cp_id> <monitor_port> <central_host> <central_port_solicitudes>")
    print("Ejemplo: python EV_CP_E.py CP01 5000 127.0.0.1 6001")
    sys.exit(1)

CP_ID = sys.argv[1]
MONITOR_PORT = int(sys.argv[2])
CENTRAL_HOST = sys.argv[3]
CENTRAL_PORT_SOLICITUDES = int(sys.argv[4])

# ============================================================
# Variables de estado del CP
# ============================================================
saludable = True
en_uso = False

# Eventos de sincronización
evento_apagado = threading.Event()
evento_menu_detener = threading.Event()

# ============================================================
# Configuración de Kafka
# ============================================================
KAFKA_BROKER = 'localhost:9092'
TOPIC_SOLICITUD = 'peticiones_engine'
TOPIC_RESPUESTA = 'respuestas_central'
TOPIC_TELEMETRIA = 'telemetry_cp'
TOPIC_TICKETS = 'tickets_cp'

# Inicializar productor Kafka
try:
    productor_kafka = Producer({'bootstrap.servers': KAFKA_BROKER})
except Exception:
    productor_kafka = None


# ============================================================
# Comunicación Kafka: envío y espera de respuesta
# ============================================================
def enviar_y_esperar_respuesta(driver_id: str, cp_id: str, timeout: float = 12.0):
    if CONSUMIDOR_RESPUESTA is None:
        return False, "kafka-no-iniciado"

    solicitud = {'driver_id': driver_id, 'cp_id': cp_id}
    
    # Envía solicitud
    if productor_kafka:
        productor_kafka.produce(TOPIC_SOLICITUD, key=driver_id, value=json.dumps(solicitud).encode('utf-8'))
        productor_kafka.flush(2)
        print(f"[Engine {CP_ID}] Solicitud enviada: {solicitud}")
    else:
        print(f"[KAFKA Fallback] {solicitud}")

    # Espera respuesta
    inicio = time.time()
    while time.time() - inicio < timeout:
        msg = CONSUMIDOR_RESPUESTA.poll(0.5)  # poll frecuente
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[Engine {CP_ID}] Error Kafka: {msg.error()}")
            continue

        try:
            respuesta = json.loads(msg.value().decode('utf-8'))
            # Filtrar SOLO respuestas para este Engine
            if (respuesta.get('cp_id') == CP_ID) and (respuesta.get('driver_id') == driver_id):
                return True, respuesta.get('mensaje') or respuesta.get('estado')
        except Exception as e:
            print(f"[Engine {CP_ID}] Error parseando respuesta: {e}")

    return False, "timeout"


def enviar_a_kafka(topic: str, payload: dict):
    mensaje = json.dumps(payload)
    if productor_kafka:
        try:
            productor_kafka.produce(topic, key=payload.get('cp_id'), value=mensaje.encode('utf-8'))
            productor_kafka.poll(0)
        except Exception as e:
            print(f"[Engine {CP_ID}] Error al enviar Kafka: {e}")
    else:
        print(f"[KAFKA:{topic}] {mensaje}")

def iniciar_consumidor_kafka():
    global CONSUMIDOR_RESPUESTA
    try:
        CONSUMIDOR_RESPUESTA = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': f'engine-{CP_ID}',
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'
        })

        CONSUMIDOR_RESPUESTA.subscribe([TOPIC_RESPUESTA])
        print(f"[Engine {CP_ID}] Consumidor Kafka iniciado (sin grupo).")
    except Exception as e:
        print(f"[Engine {CP_ID}] Error al iniciar consumidor: {e}")
        CONSUMIDOR_RESPUESTA = None
    


def escuchar_mensajes_central():
    global en_uso, hilo_telemetria

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'engine-{CP_ID}-listener',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_RESPUESTA])

    print(f"[Engine {CP_ID}] Escuchando órdenes de la central...")

    try:
        while not evento_apagado.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                if data.get('cp_id') == CP_ID:
                    estado = (data.get('estado') or data.get('mensaje') or "").lower()
                    driver_id = data.get('driver_id', 'unknown')

                    if 'autoriz' in estado or 'start' in estado:
                        if not en_uso:
                            print(f"[Engine {CP_ID}] Orden de carga recibida desde central (driver={driver_id})")
                            en_uso = True
                            hilo_telemetria = threading.Thread(
                                target=hilo_telemetria_f,
                                args=(driver_id, 7.0, None),
                                daemon=True
                            )
                            hilo_telemetria.start()
                    elif 'stop' in estado:
                        print(f"[Engine {CP_ID}] Orden de parada recibida desde central.")
                        en_uso = False

            except Exception as e:
                print(f"[Engine {CP_ID}] Error procesando mensaje de central: {e}")
    finally:
        consumer.close()
# ============================================================
# Gestión de telemetría y tickets
# ============================================================
almacen_telemetria = {}
hilo_telemetria = None


def enviar_ticket_final():
    datos = almacen_telemetria.get(CP_ID, {})
    ticket = {
        'cp_id': CP_ID,
        'driver_id': datos.get('driver_id', 'unknown'),
        'kwh': round(datos.get('kwh', 0.0), 6),
        'cost': round(datos.get('cost', 0.0), 4),
        'end_ts': time.time()
    }
    enviar_a_kafka(TOPIC_TICKETS, ticket)
    print(f"[Engine {CP_ID}] Ticket enviado: kWh={ticket['kwh']} cost={ticket['cost']}")


def hilo_telemetria_f(driver_id: str, potencia_kw: float, duracion: float = None):
    global en_uso
    inicio = time.time()
    energia = 0.0
    coste = 0.0
    precio_kwh = 0.25
    almacen_telemetria[CP_ID] = {'driver_id': driver_id, 'kwh': 0.0, 'cost': 0.0, 'start_ts': inicio}

    try:
        while en_uso and not evento_apagado.is_set():
            if duracion and (time.time() - inicio) >= duracion:
                print(f"[Engine {CP_ID}] Duración alcanzada ({duracion}s). Finalizando suministro.")
                break
            if not saludable:
                print(f"[Engine {CP_ID}] Avería detectada (saludable=False). Terminando suministro.")
                break

            incremento = potencia_kw / 3600.0
            energia += incremento
            coste += incremento * precio_kwh
            almacen_telemetria[CP_ID].update({'kwh': energia, 'cost': coste})

            print(f"[Engine {CP_ID}] Telemetría actualizada: kWh={round(energia, 6)} cost={round(coste, 4)}")

            for _ in range(10):
                if not en_uso or evento_apagado.is_set() or not saludable:
                    break
                time.sleep(0.1)
    finally:
        en_uso = False
        enviar_ticket_final()
        almacen_telemetria.pop(CP_ID, None)


# ============================================================
# Comunicación con el Monitor
# ============================================================
def manejar_solicitud_monitor(conn, activar_evento):
    global saludable, en_uso
    try:
        estado = {'cp_id': CP_ID, 'healthy': saludable, 'in_use': en_uso}
        conn.sendall(json.dumps(estado).encode('utf-8'))
        conn.settimeout(5.0)
        data = conn.recv(1024)
        if not data:
            return

        try:
            cmd = json.loads(data.decode('utf-8'))
            accion = (cmd.get('action') or "").lower()
        except Exception:
            accion = data.decode('utf-8').strip().lower()

        if accion in ('activate', 'wake', 'on'):
            if not saludable:
                conn.sendall(b'ACK:unhealthy')
            else:
                evento_menu_detener.clear()
                activar_evento.set()
                conn.sendall(b'ACK:activated')
        elif accion in ('sleep', 'off'):
            evento_menu_detener.set()
            activar_evento.clear()
            conn.sendall(b'ACK:sleep')
        else:
            conn.sendall(b'ACK:unknown')
    finally:
        conn.close()


def servidor_monitor(activar_evento):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', MONITOR_PORT))
        s.listen(1)
        s.settimeout(1.0)
        print(f"[Engine {CP_ID}] Escuchando al Monitor en puerto {MONITOR_PORT}")

        while not evento_apagado.is_set():
            try:
                conn, _ = s.accept()
                manejar_solicitud_monitor(conn, activar_evento)
            except socket.timeout:
                continue
            except OSError:
                break

        print(f"[Engine {CP_ID}] Servidor Monitor finalizado.")


# ============================================================
# Menú interactivo del operador
# ============================================================
def menu_interactivo():
    global en_uso, saludable, hilo_telemetria

    print(f"\n=== CP Engine {CP_ID} ===")
    print("1) Iniciar suministro")
    print("2) Finalizar suministro")
    print("3) Marcar fallo")
    print("4) Marcar OK")
    print("5) Mostrar estado")
    print("6) Salir")
    print("q) Parar suministro inmediato")

    evento_menu_detener.clear()

    def leer_input(prompt='> ', timeout=0.5):
        import sys, time
        sys.stdout.write('\n' + prompt)
        sys.stdout.flush()
        if _IS_WINDOWS:
            inicio = time.time()
            buffer = ''
            while not evento_menu_detener.is_set() and not evento_apagado.is_set():
                if msvcrt.kbhit():
                    ch = msvcrt.getwche()
                    if ch in ('\r', '\n'):
                        return buffer
                    buffer += ch
                else:
                    time.sleep(0.05)
                    if time.time() - inicio > timeout:
                        inicio = time.time()
            return None
        else:
            rlist, _, _ = select.select([sys.stdin], [], [], timeout)
            if rlist:
                return sys.stdin.readline().strip()
            return None

    try:
        while True:
            if evento_menu_detener.is_set() or evento_apagado.is_set():
                return 'sleep'
            comando = leer_input('> ', timeout=0.5)
            if comando is None:
                continue
            cmd = comando.lower()

            if cmd in ('1', 'start'):
                if en_uso:
                    print("[!] Ya está en uso.")
                    continue
                driver_id = input('Driver ID: ').strip() or 'DRIVER_SIM'
                try:
                    potencia = float(input('Potencia kW (default 7.0): ').strip() or '7.0')
                except Exception:
                    potencia = 7.0
                try:
                    duracion_txt = input('Duración (segundos): ').strip()
                    duracion = float(duracion_txt) if duracion_txt else None
                except Exception:
                    duracion = None

                print("[Engine] Solicitud enviada. Esperando respuesta de la Central...")
                ok, resp = enviar_y_esperar_respuesta(driver_id, CP_ID)
                if ok and isinstance(resp, str) and 'autoriz' in resp.lower():
                    en_uso = True
                    print("[Engine] Carga iniciada. Enviando telemetría...")
                    hilo_telemetria = threading.Thread(target=hilo_telemetria, args=(driver_id, potencia, duracion))
                    hilo_telemetria.start()
                else:
                    print(f"[!] Respuesta no válida o denegada: {resp}")

            elif cmd in ('2', 'stop'):
                if not en_uso:
                    print("[!] No está en uso.")
                    continue
                print("[Engine] Parando suministro...")
                en_uso = False
                if hilo_telemetria:
                    hilo_telemetria.join(timeout=5.0)
                    hilo_telemetria = None
                print("[Engine] Suministro detenido.")

            elif cmd == 'q':
                if en_uso:
                    print("[Engine] Parada inmediata del suministro.")
                    en_uso = False
                    if hilo_telemetria:
                        hilo_telemetria.join(timeout=5.0)
                        hilo_telemetria = None
                    print("[Engine] Suministro detenido")
                else:
                    print("[Engine] No hay carga en curso.")

            elif cmd in ('3', 'fail'):
                saludable = False
                print("[!] Estado marcado como no saludable.")

            elif cmd in ('4', 'ok'):
                saludable = True
                print("[✓] Estado marcado como saludable.")

            elif cmd in ('5', 'status'):
                print(f"Estado actual: saludable={saludable}, en_uso={en_uso}")

            elif cmd in ('6', 'quit'):
                print("Cerrando Engine...")
                en_uso = False
                if hilo_telemetria:
                    hilo_telemetria.join(timeout=5.0)
                    hilo_telemetria = None
                evento_apagado.set()
                return 'exit'

            else:
                print("Comando no reconocido.")
    except KeyboardInterrupt:
        print("[Engine] Interrupción detectada. Cerrando menú.")
        en_uso = False
        if hilo_telemetria:
            hilo_telemetria.join(timeout=5.0)
            hilo_telemetria = None
        return 'sleep'


# ============================================================
# Ejecución
# ============================================================
if __name__ == '__main__':
    iniciar_consumidor_kafka()
    hilo_listener = threading.Thread(target=escuchar_mensajes_central, daemon=True)
    hilo_listener.start()
    

    activar_evento = threading.Event()
    signal.signal(signal.SIGINT, signal.default_int_handler)

    hilo_monitor = threading.Thread(target=servidor_monitor, args=(activar_evento,))
    hilo_monitor.start()

    print(f"[Engine {CP_ID}] Iniciado (saludable={saludable})")
    print(f"[Engine {CP_ID}] Fuera de servicio. Esperando activación del Monitor.")

    try:
        while not evento_apagado.is_set():
            activar_evento.wait()
            if evento_apagado.is_set():
                break
            if not saludable:
                print(f"[Engine {CP_ID}] Activación recibida pero CP no saludable.")
                activar_evento.clear()
                time.sleep(0.1)
                continue
            print("[Engine] Activado por Monitor. Iniciando menú.")
            resultado = menu_interactivo()
            if resultado == 'exit':
                break
            activar_evento.clear()
            print("[Engine] Volviendo a modo espera.")
    finally:
        evento_apagado.set()
        hilo_monitor.join(timeout=2.0)
        print('[Engine] Apagado completo.')
        if 'hilo_listener' in locals() and hilo_listener.is_alive():
            hilo_listener.join(timeout=2.0)
        if productor_kafka:
            try:
                productor_kafka.flush(3)
                print("[Engine] Kafka flush completado antes de salir.")
            except Exception as e:
                print(f"[Engine] Error al hacer flush Kafka: {e}")
        sys.exit(0)
        