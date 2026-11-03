# API REST + mapa web del consumo 3. El código se monta desde la carpeta del proyecto (docker-compose.yml),
# así que la imagen solo trae Python y las librerías.
FROM python:3.12-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /opt/sismo/consumo/api_mapa
EXPOSE 8090
CMD ["python", "app.py"]
