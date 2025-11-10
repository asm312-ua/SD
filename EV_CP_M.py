import socket
import json
import sys
import time
import threading

# ============================================================
# Validación de argumentos
# ============================================================
if len(sys.argv) != 6:
    print("Uso: python EV_CP_M.py <cp_id> <engine_host> <engine_port> <central_host> <cmd_port>")
    print("Ej: python EV_CP_M.py CP01 127.0.0.1 5000 192.168.0.100 6002")
    sys.exit(1)

CP_ID = sys.argv[1]
ENGINE_HOST = sys.argv[2]
ENGINE_PORT = int(sys.argv[3])
CENTRAL_HOST = sys.argv[4]
CENTRAL_PORT_ESTADOS = 6000
MONITOR_CMD_PORT = int(sys.argv[5])

# ============================================================
# Constantes y estado global
# ============================================================
SOCKET_TIMEOUT = 2
SOCKET_BUFFER = 8192

# La Central puede establecer una orden persistente (override)
# Posibles valores: None | 'activate' | 'sleep'
central_override = None


# ============================================================
# Utilidades generales
# ============================================================
def obtener_ip_local() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ============================================================
# Comunicación con el Engine
# ============================================================
def obtener_estado_engine() -> dict:
    global central_override
    try:
        with socket.create_connection((ENGINE_HOST, ENGINE_PORT), timeout=SOCKET_TIMEOUT) as s:
            s.settimeout(5.0)
            data = s.recv(SOCKET_BUFFER)
            if not data:
                raise RuntimeError("sin datos del Engine")

            estado = json.loads(data.decode('utf-8'))
            engine_healthy = bool(estado.get('healthy'))
            action_to_send = None
            deferred = False

            # Determinar la acción según override o estado
            if central_override == 'sleep':
                action_to_send = 'sleep'
            elif central_override == 'activate':
                if engine_healthy:
                    action_to_send = 'activate'
                else:
                    # Si el Engine no está saludable, aplazar activación
                    action_to_send = 'sleep'
                    deferred = True
            else:
                # Sin override: comportamiento automático
                action_to_send = 'activate' if engine_healthy else 'sleep'

            # Enviar la acción al Engine
            try:
                s.sendall(json.dumps({'action': action_to_send}).encode('utf-8'))
                s.settimeout(2.0)
                ack = s.recv(SOCKET_BUFFER)
                if ack:
                    print(f"[Monitor {CP_ID}] ACK Engine: {ack.decode(errors='ignore')}")
            except Exception:
                pass

            # Completar estado con metadatos para enviar a la Central
            estado.update({
                'ip': obtener_ip_local(),
                'cmd_port': MONITOR_CMD_PORT,
                'cp_id': CP_ID,
                'action_sent': action_to_send,
                'central_override': central_override,
                'override_deferred': deferred
            })
            return estado

    except Exception as e:
        # Si el Engine no responde, informar estado de fallo
        print(f"[Monitor {CP_ID}] Error leyendo Engine: {e}")
        return {
            'cp_id': CP_ID,
            'healthy': False,
            'in_use': False,
            'ip': obtener_ip_local(),
            'cmd_port': MONITOR_CMD_PORT,
            'active': central_override != 'sleep',
            'action_sent': 'none',
            'central_override': central_override,
            'override_deferred': False
        }


# ============================================================
# Comunicación con la Central
# ============================================================
def enviar_a_central(estado: dict):
    try:
        with socket.create_connection((CENTRAL_HOST, CENTRAL_PORT_ESTADOS), timeout=SOCKET_TIMEOUT) as s:
            msg = json.dumps(estado) + '\n'
            s.sendall(msg.encode('utf-8'))
    except Exception as e:
        print(f"[Monitor {CP_ID}] Error al enviar a CENTRAL: {e}")


# ============================================================
# Recepción de comandos de la Central
# ============================================================
def manejar_comando_central(conn: socket.socket, addr):
    global central_override
    with conn:
        try:
            data = conn.recv(SOCKET_BUFFER)
            if not data:
                return

            msg = json.loads(data.decode('utf-8'))
            action = (msg.get('action', '') or '').lower()
            cp = msg.get('cp_id') or CP_ID
            print(f"[Monitor {CP_ID}] Orden de Central para {cp}: {action}")

            if action == 'activate':
                central_override = 'activate'
            elif action in ('sleep', 'off'):
                central_override = 'sleep'
            elif action in ('clear', 'none', ''):
                central_override = None
            # Otros comandos se ignoran pero se responde igual

            conn.sendall(json.dumps({
                'status': 'ok',
                'central_override': central_override
            }).encode('utf-8'))

        except Exception as e:
            print(f"[Monitor {CP_ID}] Error manejando comando Central: {e}")


def servidor_comandos():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('0.0.0.0', MONITOR_CMD_PORT))
        s.listen(4)
        print(f"[Monitor {CP_ID}] Escuchando comandos de Central en puerto {MONITOR_CMD_PORT}")

        while True:
            conn, addr = s.accept()
            threading.Thread(
                target=manejar_comando_central,
                args=(conn, addr),
                daemon=True
            ).start()


# ============================================================
# Bucle principal
# ============================================================
def main():
    print(f"[Monitor {CP_ID}] Iniciado con central_override={central_override}")
    threading.Thread(target=servidor_comandos, daemon=True).start()

    try:
        while True:
            estado = obtener_estado_engine()
            enviar_a_central(estado)
            time.sleep(1)  # frecuencia de actualización
    except KeyboardInterrupt:
        print("\n[Monitor] Apagando...")


# ============================================================
# Ejecución
# ============================================================
if __name__ == '__main__':
    main()