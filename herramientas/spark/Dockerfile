# Imagen de Spark con el driver JDBC de ClickHouse ("all": incluye sus dependencias), para que el
# job de LastQuake cargue el Data Warehouse en cada micro-batch.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
ADD --chmod=644 https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.11.0/clickhouse-jdbc-0.11.0-all.jar /opt/spark/jars/
