from flask import Flask, render_template, jsonify
import json
import time

app = Flask(__name__)

IP_CENTRAL = 'localhost'
DB_FILE = "../basedatos.json"

def cargar_cp_basedatos():
    try:
        with open(DB_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}

@app.route("/")
def index():
    cps = cargar_cp_basedatos()
    return render_template("index.html", cps=cps)

@app.route("/api/cps")
def api_cps():
    cps = cargar_cp_basedatos()
    return jsonify(cps)

if __name__ == "__main__":
    print(f"[Servidor gráfico] Ejecutando en http://{IP_CENTRAL}:8080 ...")
    app.run(host=IP_CENTRAL, port=8080, debug=True)
