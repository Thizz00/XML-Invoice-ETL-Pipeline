FROM apache/spark:3.5.0-python3

USER root

RUN apt-get update && apt-get install -y wget && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar -P /opt/spark/jars/ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/data/xml && \
    mkdir -p /opt/spark/app && \
    chmod -R 777 /opt/spark/data

COPY scripts/etl_process_invoices.py /opt/spark/app/etl_process_invoices.py

WORKDIR /opt/spark/app

ENTRYPOINT ["/bin/sh", "-c", "\
    LOG_DIR=/opt/spark/data/logs/$(date +%Y%m%d)/spark && \
    mkdir -p $LOG_DIR && \
    /opt/spark/bin/spark-submit \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.sql.xml.samplingRatio=0.1 \
    /opt/spark/app/etl_process_invoices.py \
    2>&1 | tee $LOG_DIR/spark_$(date +%H%M%S).log"]