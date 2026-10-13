# bda/hadoop: Hadoop 3.3.6 on Java 8. Base for every Hadoop daemon and for bda/stack.
FROM eclipse-temurin:8-jdk-noble

ARG TARGETARCH
ARG HADOOP_VERSION=3.3.6

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-venv curl netcat-openbsd procps less \
 && rm -rf /var/lib/apt/lists/*

# setup.sh downloads the arch-specific tarball into docker/cache and verifies its sha512.
COPY cache/hadoop-${HADOOP_VERSION}-${TARGETARCH}.tar.gz /tmp/hadoop.tgz
RUN mkdir -p /opt/hadoop \
 && tar -xzf /tmp/hadoop.tgz -C /opt/hadoop --strip-components=1 \
 && rm -rf /tmp/hadoop.tgz /opt/hadoop/share/doc

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    TZ=UTC
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

COPY hadoop/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["entrypoint.sh"]
CMD ["bash"]
