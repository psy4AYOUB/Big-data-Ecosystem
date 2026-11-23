# Job Spark Structured Streaming de labsecu
# Image officielle Spark 3.5 (Scala 2.12, Java 17, Python 3)
ARG SPARK_VERSION=3.5.8
FROM spark:${SPARK_VERSION}-scala2.12-java17-python3-ubuntu
LABEL org.opencontainers.image.source="https://github.com/psy4AYOUB/Big-data-Ecosystem"
LABEL org.opencontainers.image.description="labsecu - job Spark Structured Streaming (detection R1 a R4)"

USER root
RUN mkdir -p /opt/labsecu /checkpoints /opt/spark/ivy \
 && chown -R spark:spark /opt/labsecu /checkpoints /opt/spark/ivy
COPY --chown=spark:spark app/ /opt/labsecu/
COPY --chown=spark:spark lancer.sh /opt/labsecu/lancer.sh
RUN chmod +x /opt/labsecu/lancer.sh
USER spark
WORKDIR /opt/labsecu

ENTRYPOINT ["/opt/labsecu/lancer.sh"]
