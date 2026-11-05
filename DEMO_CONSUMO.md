# Guía de demo: consumos 1 y 2

Se ejecuta desde la carpeta del proyecto, con Docker Desktop abierto y el `.venv` activo
(`source .venv/Scripts/activate`). Antes de partir, una sola vez:

```bash
git pull                              # trae ClickHouse y el resto del almacenamiento
pip install -r requirements.txt tzdata
```

Todos los comandos de Docker usan los dos archivos de compose (el principal y el de consumo).
Para no escribirlo cada vez, en Bash:

```bash
alias dc='docker compose -f docker-compose.yml -f docker-compose.consumo.yml'
```

---

## Consumo 1: Alertas (MongoDB → terminal, correo, Slack, Telegram)

**Qué demuestra:** el streaming de EMSC (Kafka) llega a MongoDB y un bot lo consume: si un sismo cumple las
reglas (magnitud mínima y cercanía a una ciudad, usando el índice geoespacial `2dsphere`), avisa en segundos.

```bash
dc --profile kafka --profile alertas up -d       # Kafka, MongoDB, Mongo Express y Mailpit
python fuentes/emsc_streaming/productor_kafka.py --kafka localhost:9092 --duracion 900      # terminal 1
python fuentes/emsc_streaming/consumidor_mongodb.py --kafka localhost:9092 --continuo        # terminal 2
python consumo/alertas/alertas.py --continuo                                                 # terminal 3 (el bot)
```

**Qué mostrar:**
1. Terminal 1: el sismo llega por WebSocket. Terminal 2: queda en MongoDB (`MongoDB <- create ...`).
   Mongo Express (http://localhost:8082, admin / sismologia) → colección `emsc_eventos`.
2. Terminal 3: el bot imprime el sismo y la alerta aparece en la **bandeja de Mailpit**
   (http://localhost:8025): asunto, hora de Chile, ciudad más cercana y enlace al mapa.
3. Reglas por defecto: M ≥ 4,5 a ≤ 300 km de una de 12 ciudades. Se ajustan por línea de comandos
   (`--umbral 5 --radio 200 --ciudades Santiago Calama`).
4. No repite alertas: cada sismo se avisa una vez (colección `alertas_enviadas`). Si EMSC corrige la
   magnitud en 0,5 o más, llega una **ACTUALIZACIÓN**.

**Si durante la presentación no ocurre ningún sismo cerca de Chile** (es lo normal), tienen dos salidas:

```bash
python consumo/alertas/alertas.py --probar                                 # alerta de prueba con el último sismo guardado
python consumo/alertas/alertas.py --umbral 4 --radio 20000 --continuo      # avisa sismos de todo el mundo
```

**Slack y Telegram (opcional):** copien `consumo/alertas/.env.example` como `.env` y completen el
webhook de Slack o el token del bot de Telegram. El bot los usa solo si están definidos.

Para consultar lo guardado sin Kafka: `dc --profile mongo --profile alertas up -d` y ejecutar `--probar`.

---

## Consumo 2: Dashboard de BI (ClickHouse → Grafana)

**Qué demuestra:** los testimonios ciudadanos de LastQuake, procesados por Spark y guardados en el Data
Warehouse (modelo estrella), se consumen en un dashboard con consultas SQL sobre la vista `v_testimonios`.

```bash
dc --profile spark --profile dashboard up -d --build     # Spark + ClickHouse + Grafana
docker logs -f sismo-spark                                # esperar "[warehouse] micro-batch N ..."
python fuentes/emsc_lastquake/sondeo.py --desde 2026-09-01 --ciclos 1
```

Sin Spark, con testimonios ya cargados: `dc --profile warehouse --profile dashboard up -d`.

Abran **http://localhost:3000**. El dashboard "Sismos sentidos en Chile" se abre solo (la primera vez
Grafana descarga el plugin de ClickHouse: ~1 minuto).

**Qué mostrar:**
1. Los 4 indicadores: testimonios, sismos con testimonios, intensidad promedio (EMS-98) y distancia mediana.
2. **Testimonios por macrozona** y **canal de reporte** (app, web, teléfono): dónde y cómo reporta la gente.
3. **Atenuación:** la intensidad promedio cae a medida que aumenta la distancia al epicentro.
4. **Testimonios por hora:** cada pico es un sismo; los reportes llegan en los minutos siguientes.
5. Con Spark corriendo, cada micro-batch (1 minuto) actualiza los paneles. El dashboard se refresca solo
   cada minuto (o con el botón de recarga).
6. Para mostrar que es SQL real: panel → *Edit* → se ve la consulta sobre `sismologia.v_testimonios`.
   Cambien el rango de tiempo arriba a la derecha y los paneles se recalculan.
7. Los testimonios repetidos (Spark puede reenviar un micro-batch) no se cuentan dos veces: la vista usa `FINAL`.

**Si los paneles salen vacíos:** revisen el rango de tiempo (por defecto 90 días) y que ClickHouse tenga
datos: http://localhost:8123/play (admin / sismologia) → `SELECT count() FROM sismologia.v_testimonios`.
Si da 0, borren `datos/_estado/lastquake.json` y repitan el sondeo (ver sección 9 del `DEMO.md`).

---

## Detener todo

```bash
dc --profile kafka --profile alertas --profile spark --profile dashboard down
```

No usen `-v` a menos que quieran borrar los datos de MongoDB y ClickHouse.
