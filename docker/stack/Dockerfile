# bda/stack: every Hadoop-ecosystem client and server that runs on Java 8.
# Used by edge, hive-metastore, hiveserver2, hbase-master, hbase-regionserver.

# --- unpack + prune in a throwaway stage so deleted files never land in a layer ---
FROM bda/hadoop:3.3.6 AS unpack
COPY cache/apache-hive-3.1.3-bin.tar.gz cache/pig-0.18.0.tar.gz cache/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz \
     cache/apache-mahout-distribution-0.13.0.tar.gz cache/spark-3.5.9-bin-hadoop3.tgz \
     cache/hbase-2.5.16-hadoop3-bin.tar.gz cache/postgresql-42.7.4.jar cache/commons-lang-2.6.jar cache/json-20090211.jar /tmp/
WORKDIR /opt
RUN set -e; \
    for p in "hive apache-hive-3.1.3-bin.tar.gz" "pig pig-0.18.0.tar.gz" "sqoop sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz" \
             "mahout apache-mahout-distribution-0.13.0.tar.gz" "spark spark-3.5.9-bin-hadoop3.tgz" \
             "hbase hbase-2.5.16-hadoop3-bin.tar.gz"; do \
      set -- $p; mkdir "$1"; tar -xzf "/tmp/$2" -C "$1" --strip-components=1; \
    done; \
    # Pig runs on MapReduce here: drop its Spark engine jars, docs, sources and tests (~350 MB)
    rm -rf pig/lib/spark pig/docs pig/src pig/legacy pig/test pig/tutorial pig/contrib; \
    rm -rf mahout/docs mahout/*spark* mahout/*viennacl* mahout/*scala* mahout/flink mahout/h2o; \
    rm -rf hive/jdbc hbase/docs spark/R spark/examples spark/data spark/yarn sqoop/docs sqoop/src; \
    # Workaround: Hive 3.1.3 ships guava 19, Hadoop 3.3.6 needs 27 (NoSuchMethodError on startup otherwise)
    rm hive/lib/guava-19.0.jar; \
    cp /opt/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar hive/lib/; \
    # Workaround: Sqoop 1.4.7 needs commons-lang 2 (removed in Hadoop 3.3); JDBC driver for Postgres
    cp /tmp/commons-lang-2.6.jar sqoop/lib/; \
    # Workaround: saved Sqoop jobs NPE with Hive's clean-room org.json (json-1.8.jar); real org.json wins in sqoop/lib
    cp /tmp/json-20090211.jar sqoop/lib/; \
    cp /tmp/postgresql-42.7.4.jar sqoop/lib/; cp /tmp/postgresql-42.7.4.jar hive/lib/; cp /tmp/postgresql-42.7.4.jar spark/jars/

# --- final image ---
FROM bda/hadoop:3.3.6
COPY --from=unpack /opt/hive   /opt/hive
COPY --from=unpack /opt/pig    /opt/pig
COPY --from=unpack /opt/sqoop  /opt/sqoop
COPY --from=unpack /opt/mahout /opt/mahout
COPY --from=unpack /opt/spark  /opt/spark
COPY --from=unpack /opt/hbase  /opt/hbase

# Python tools for PySpark, the catalogue seeder, the Kafka replayer and the benchmark charts.
# setuptools: Python 3.12 removed distutils, which pyspark.ml 3.5 still imports (setuptools shims it).
RUN python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir \
      setuptools==75.6.0 numpy==2.1.3 pandas==2.2.3 matplotlib==3.9.2 psycopg2-binary==2.9.10 requests==2.32.3 \
      confluent-kafka==2.6.1 pytest==8.3.3

ENV HIVE_HOME=/opt/hive HCAT_HOME=/opt/hive/hcatalog PIG_HOME=/opt/pig SQOOP_HOME=/opt/sqoop \
    MAHOUT_HOME=/opt/mahout SPARK_HOME=/opt/spark HBASE_HOME=/opt/hbase \
    HADOOP_COMMON_HOME=/opt/hadoop HADOOP_HDFS_HOME=/opt/hadoop HADOOP_YARN_HOME=/opt/hadoop \
    ACCUMULO_HOME=/nonexistent ZOOKEEPER_HOME=/opt/hbase
ENV PATH=/opt/venv/bin:$HIVE_HOME/bin:$PIG_HOME/bin:$SQOOP_HOME/bin:$MAHOUT_HOME/bin:$SPARK_HOME/bin:$HBASE_HOME/bin:$PATH
