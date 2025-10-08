# Flink 2.0.0, Scala 2.12, Java 17
FROM flink:2.0.0-scala_2.12-java17

COPY target/pricing-job-*.jar /opt/flink/usrlib/pricing-job.jar