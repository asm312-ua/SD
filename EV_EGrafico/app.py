from flask import Flask, render_template, jsonify
import json
import time

app = Flask(__name__)

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
    print("[Servidor gráfico] Ejecutando en http://localhost:8080 ...")
    app.run(host="0.0.0.0", port=8080, debug=True)
