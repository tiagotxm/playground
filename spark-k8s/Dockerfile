# Base image with Spark
FROM apache/spark:4.0.1-scala2.13-java17-python3-r-ubuntu

USER root

# Setup for the Prometheus JMX exporter.
ENV JMX_EXPORTER_AGENT_VERSION=1.5.0
ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar /opt/spark/jars
RUN chmod 644 /opt/spark/jars/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar

USER spark