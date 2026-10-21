"""Crea en Apache NiFi el flujo de ingesta del CSN Chile (miniSEED), usando la API REST de NiFi.

Ejecutar con NiFi ya levantado (docker compose --profile nifi up -d, esperar ~2 minutos):
    python herramientas/nifi/crear_flujo.py

Flujo creado dentro del grupo "Ingesta CSN Chile - miniSEED":

  GenerateFlowFile (cada 5 min, lista de estaciones)
    -> SplitText (una estación por FlowFile)
    -> ExtractText (atributos sta y cha)
    -> UpdateAttribute (ventana de tiempo t0 / t1, 10 a 5 minutos atrás)
    -> InvokeHTTP (GET al servicio FDSN dataselect del CSN)
    -> RouteOnAttribute (validación: HTTP 200 y archivo no vacío)
         con_datos -> UpdateAttribute (nombre del archivo) -> PutFile (datos/raw/csn_chile/nifi/...)
         sin datos -> LogAttribute (control: queda registrado)
    errores HTTP y de escritura -> LogAttribute

El flujo queda detenido: en la demo se inicia con el botón Play del grupo.
"""
from __future__ import annotations

import argparse
import sys
import time

import requests
import urllib3

urllib3.disable_warnings()  # NiFi usa un certificado autofirmado

API = "https://localhost:8443/nifi-api"
USUARIO, CLAVE = "admin", "sismologia2026"
ESTACIONES = ["BO01:HHZ", "BO02:HHZ", "VA05:HNZ", "B05O:HNZ", "AC01:HHZ", "AP01:HHZ"]
FDSN = "https://owl.csn.uchile.cl/fdsn95/dataselect/1/query"
FMT = "format(\"yyyy-MM-dd'T'HH:mm:ss\", \"GMT\")"
ALIAS = {"HTTP URL": "Remote URL", "Socket Read Timeout": "Read Timeout"}

S = requests.Session()
S.verify = False


def api(metodo: str, ruta: str, **kw):
    r = S.request(metodo, API + ruta, timeout=60, **kw)
    if r.status_code >= 400:
        sys.exit(f"Error {r.status_code} en {metodo} {ruta}:\n{r.text[:800]}")
    return r.json() if r.text and r.headers.get("content-type", "").startswith("application/json") else r.text


def conectar():
    for intento in range(30):
        try:
            r = S.post(API + "/access/token", data={"username": USUARIO, "password": CLAVE}, timeout=10)
            if r.status_code == 201:
                S.headers["Authorization"] = f"Bearer {r.text}"
                print("Conectado a NiFi")
                return
        except requests.ConnectionError:
            pass
        print(f"Esperando a que NiFi arranque... ({intento + 1}/30)")
        time.sleep(10)
    sys.exit("NiFi no respondió en https://localhost:8443. ¿Está levantado el contenedor?")


def crear_procesador(grupo: str, tipo: str, nombre: str, x: int, y: int,
                     propiedades: dict | None = None, auto_terminar: list | None = None,
                     programacion: str | None = None) -> str:
    cuerpo = {"revision": {"version": 0},
              "component": {"type": tipo, "name": nombre, "position": {"x": x, "y": y}}}
    p = api("POST", f"/process-groups/{grupo}/processors", json=cuerpo)
    pid = p["id"]
    # Los nombres internos de las propiedades cambian entre versiones de NiFi: se buscan por
    # nombre visible y, si la propiedad no existe, se envía como propiedad dinámica.
    descriptores = p["component"]["config"]["descriptors"]
    por_nombre_visible = {d["displayName"]: d["name"] for d in descriptores.values()}
    por_nombre_visible.update({nombre: nombre for nombre in descriptores})
    for nuevo, antiguo in ALIAS.items():  # nombres de NiFi 1.x
        if nuevo not in por_nombre_visible and antiguo in por_nombre_visible:
            por_nombre_visible[nuevo] = por_nombre_visible[antiguo]
    props = {por_nombre_visible.get(k, k): v for k, v in (propiedades or {}).items()}
    config = {"properties": props}
    if auto_terminar:
        config["autoTerminatedRelationships"] = auto_terminar
    if programacion:
        config["schedulingPeriod"] = programacion
    api("PUT", f"/processors/{pid}", json={"revision": p["revision"],
                                            "component": {"id": pid, "config": config}})
    print(f"  + {nombre}")
    return pid


def conectar_procesadores(grupo: str, origen: str, destino: str, relaciones: list[str]):
    api("POST", f"/process-groups/{grupo}/connections", json={
        "revision": {"version": 0},
        "component": {
            "source": {"id": origen, "groupId": grupo, "type": "PROCESSOR"},
            "destination": {"id": destino, "groupId": grupo, "type": "PROCESSOR"},
            "selectedRelationships": relaciones,
            "backPressureObjectThreshold": 1000,
        }})


def main(iniciar: bool):
    conectar()
    raiz = api("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
    for g in api("GET", f"/process-groups/{raiz}/process-groups")["processGroups"]:
        if g["component"]["name"].startswith("Ingesta CSN Chile"):
            sys.exit("El grupo 'Ingesta CSN Chile' ya existe. Bórralo en la interfaz de NiFi para recrearlo.")
    grupo = api("POST", f"/process-groups/{raiz}/process-groups", json={
        "revision": {"version": 0},
        "component": {"name": "Ingesta CSN Chile - miniSEED", "position": {"x": 0, "y": 0}}})["id"]
    print("Grupo creado: Ingesta CSN Chile - miniSEED")

    std = "org.apache.nifi.processors.standard."
    generar = crear_procesador(grupo, std + "GenerateFlowFile", "1. Disparo cada 5 min (lista de estaciones)", 0, 0,
                               {"Custom Text": "\n".join(ESTACIONES), "Data Format": "Text"},
                               programacion="5 min")
    dividir = crear_procesador(grupo, std + "SplitText", "2. Una estacion por FlowFile", 0, 200,
                               {"Line Split Count": "1", "Remove Trailing Newlines": "true"},
                               auto_terminar=["original", "failure"])
    extraer = crear_procesador(grupo, std + "ExtractText", "3. Atributos sta y cha", 0, 400,
                               {"sta": r"^([A-Z0-9]+):", "cha": r":([A-Z0-9]+)"},
                               auto_terminar=["unmatched"])
    ventana = crear_procesador(grupo, "org.apache.nifi.processors.attributes.UpdateAttribute",
                               "4. Ventana de tiempo (t0, t1)", 0, 600,
                               {"t0": "${now():toNumber():minus(600000):" + FMT + "}",
                                "t1": "${now():toNumber():minus(300000):" + FMT + "}"})
    http = crear_procesador(grupo, std + "InvokeHTTP", "5. GET FDSN dataselect (CSN)", 0, 800,
                            {"HTTP Method": "GET",
                             "HTTP URL": FDSN + "?net=C1&sta=${sta}&cha=${cha}&starttime=${t0}&endtime=${t1}",
                             "Socket Read Timeout": "60 secs"},
                            auto_terminar=["Original"])
    validar = crear_procesador(grupo, std + "RouteOnAttribute", "6. Validacion: HTTP 200 y no vacio", 0, 1000,
                               {"con_datos":"${invokehttp.status.code:equals('200'):and(${fileSize:gt(0)})}"})
    nombre = crear_procesador(grupo, "org.apache.nifi.processors.attributes.UpdateAttribute",
                              "7. Nombre del archivo", 0, 1200,
                              {"filename": "C1.${sta}.${cha}.${t0:replace(':', '')}.mseed",
                               "fecha": "${t0:substring(0, 10)}"})
    guardar = crear_procesador(grupo, std + "PutFile", "8. Guardar miniSEED (capa bronce)", 0, 1400,
                               {"Directory": "/opt/sismo/datos/raw/csn_chile/nifi/estacion=${sta}/fecha=${fecha}",
                                "Conflict Resolution Strategy": "replace",
                                "Create Missing Directories": "true"},
                               auto_terminar=["success"])
    registro = crear_procesador(grupo, std + "LogAttribute", "Control: sin datos o errores", 600, 1000,
                                {"Log Level": "warn"}, auto_terminar=["success"])

    conectar_procesadores(grupo, generar, dividir, ["success"])
    conectar_procesadores(grupo, dividir, extraer, ["splits"])
    conectar_procesadores(grupo, extraer, ventana, ["matched"])
    conectar_procesadores(grupo, ventana, http, ["success"])
    conectar_procesadores(grupo, http, validar, ["Response"])
    conectar_procesadores(grupo, http, registro, ["Failure", "No Retry"])
    conectar_procesadores(grupo, http, http, ["Retry"])          # reintento automático
    conectar_procesadores(grupo, validar, nombre, ["con_datos"])
    conectar_procesadores(grupo, validar, registro, ["unmatched"])
    conectar_procesadores(grupo, nombre, guardar, ["success"])
    conectar_procesadores(grupo, guardar, registro, ["failure"])
    print("Conexiones creadas")

    invalidos = [p["component"]["name"] + ": " + "; ".join(p["component"].get("validationErrors") or [])
                 for p in api("GET", f"/process-groups/{grupo}/processors")["processors"]
                 if p["component"].get("validationStatus") == "INVALID"]
    if invalidos:
        print("\nAVISO, procesadores con configuración inválida:\n  " + "\n  ".join(invalidos))
    elif iniciar:
        api("PUT", f"/flow/process-groups/{grupo}", json={"id": grupo, "state": "RUNNING"})
        print("Flujo iniciado")
    print("\nListo. Abre https://localhost:8443/nifi (usuario admin / clave sismologia2026)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--iniciar", action="store_true", help="dejar el flujo corriendo al terminar")
    main(ap.parse_args().iniciar)
