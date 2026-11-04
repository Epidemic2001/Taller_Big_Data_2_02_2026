# Endpoint de Machine Learning del consumo 4. El código se monta desde la carpeta del proyecto
# (docker-compose.yml), así que la imagen solo trae Python y las librerías.
FROM python:3.12-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /opt/sismo/consumo/api_ml
EXPOSE 8091
CMD ["python", "app.py"]
