FROM flink:1.15.1-scala_2.12-java11
# flink:1.13.0-scala_2.11 => versão do MV ?? 
# flink:1.13.0 => versão do MV ?? 
# flink:1.14.5-scala_2.11-java11 
# flink:1.10.0-scala_2.11

RUN set -ex; \
  apt-get update; \
  apt-get -y install python3; \
  apt-get -y install python3-pip; \
  apt-get -y install python3-dev; \
  ln -s /usr/bin/python3 /usr/bin/python; \
  ln -s /usr/bin/pip3 /usr/bin/pip


RUN set -ex; \
  apt-get update; \
  python -m pip install --upgrade pip; \
  pip install apache-flink; \
  pip install kafka-python

ARG FLINK_VERSION=1.10.0

RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/${FLINK_VERSION}/flink-csv-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch6_2.11/${FLINK_VERSION}/flink-sql-connector-elasticsearch6_2.11-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/${FLINK_VERSION}/flink-sql-connector-kafka_2.11-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://dl.bintray.com/streamnative/maven/io/streamnative/connectors/pulsar-flink-connector-2.11-1.12/2.7.0/pulsar-flink-connector-2.11-1.12-2.7.0.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro/${FLINK_VERSION}/flink-sql-avro-${FLINK_VERSION}.jar; \

# Create data folders
    mkdir -p /opt/data; \
    echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml; \
    echo "rest.port: 8088" >> /opt/flink/conf/flink-conf.yaml;
