# ── 1단계: Maven 빌드 ─────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# 의존성 캐시 레이어 (소스 변경 시 재다운로드 방지)
COPY pom.xml .
COPY dev/pom.xml dev/
RUN mvn dependency:go-offline -pl dev -q

# 소스 빌드
COPY dev/src dev/src
RUN mvn package -pl dev -DskipTests -q


# ── 2단계: Spark 런타임 이미지 ────────────────────────────────────────────────
FROM apache/spark:3.4.1-scala2.12-java17-python3-ubuntu

USER root

# S3A / MinIO 연동 JAR (Spark 기본 이미지에 없는 버전 직접 추가)
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# 빌드된 fat JAR 배치
COPY --from=builder /build/dev/target/dev.jar /opt/spark/jars/dev.jar

USER 185
