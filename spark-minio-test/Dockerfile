# ── 1단계: sbt 빌드 ────────────────────────────────────────────────────────────
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.5_8_1.9.7_2.12.17 AS builder

WORKDIR /app

# 의존성 캐시 레이어 (소스 변경 시 재다운로드 방지)
COPY project/build.properties project/
COPY project/plugins.sbt       project/
COPY build.sbt                 .
RUN sbt update

# 소스 복사 및 assembly jar 빌드
COPY src/ src/
RUN sbt assembly


# ── 2단계: Spark 런타임 이미지 ────────────────────────────────────────────────
FROM apache/spark:3.4.1-scala2.12-java17-python3-ubuntu

USER root

# hadoop-aws / aws-sdk jar 추가 (Spark 이미지에 없는 버전 직접 추가)
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# 빌드된 fat jar 복사
COPY --from=builder /app/target/scala-2.12/spark-minio-test-1.0.0.jar /opt/spark/jars/spark-minio-test.jar

# 실행 스크립트
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

USER 185

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
