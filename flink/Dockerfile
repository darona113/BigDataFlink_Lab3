FROM flink:1.17.2-scala_2.12-java11

USER root
RUN apt-get update     && apt-get install -y --no-install-recommends python3 python3-pip curl wget     && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.17.2 psycopg2-binary==2.9.9

RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar       https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar     && wget -q -O /opt/flink/lib/flink-connector-jdbc-3.1.2-1.17.jar       https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar     && wget -q -O /opt/flink/lib/postgresql-42.7.3.jar       https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

USER flink
WORKDIR /opt/flink
