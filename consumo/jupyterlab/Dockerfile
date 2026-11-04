# JupyterLab del consumo 5 (consultas ad-hoc y exploración). Los notebooks se montan desde la carpeta
# del proyecto (docker-compose.yml), así que la imagen solo trae Python y las librerías.
FROM python:3.12-slim
COPY requirements.txt /tmp/requirements.txt
# La extensión httpfs deja que DuckDB lea Parquet directo de MinIO (S3). Se instala al construir la
# imagen para no descargarla en cada sesión.
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && python -c "import duckdb; duckdb.sql('INSTALL httpfs')"
# Sin el aviso de noticias de Jupyter al abrir (pide permiso para consultar internet)
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
WORKDIR /opt/sismo/consumo/jupyterlab/notebooks
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.root_dir=/opt/sismo/consumo/jupyterlab/notebooks"]
