# ── 1단계: Maven 빌드 (thin JAR) ─────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
COPY dev/pom.xml dev/
RUN mvn dependency:go-offline -pl dev -q

COPY dev/src dev/src
RUN mvn package -pl dev -DskipTests -q


# ── 2단계: Spark 런타임 이미지 ────────────────────────────────────────────────
FROM apache/spark:3.4.1-scala2.12-java17-python3-ubuntu

USER root

# S3A / MinIO JAR
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Iceberg JAR (mainClass는 CR에서 지정하므로 shade 없이 이미지에 직접 배치)
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.3/iceberg-spark-runtime-3.4_2.12-1.4.3.jar" \
    -o /opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.4.3.jar

# 컴파일된 thin JAR
COPY --from=builder /build/dev/target/dev-1.0.0.jar /opt/spark/jars/dev.jar

USER 185
