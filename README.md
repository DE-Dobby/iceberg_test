# iceberg_test

## English

This repository is for testing **Apache Spark**, **Apache Iceberg**, and **MinIO** integration.

It covers various test scenarios to validate the behavior of Iceberg table format on top of Spark, using MinIO as the S3-compatible object storage backend.

### What is being tested
- Apache Spark + Apache Iceberg integration
- MinIO as S3-compatible object storage
- Iceberg table operations (create, read, write, schema evolution, time travel, etc.)

---

## 한국어

이 저장소는 **Apache Spark**, **Apache Iceberg**, **MinIO** 연동을 테스트하기 위한 프로젝트입니다.

Spark 위에서 Iceberg 테이블 포맷이 올바르게 동작하는지 검증하며, S3 호환 오브젝트 스토리지인 MinIO를 백엔드로 사용합니다.

### 테스트 항목
- Apache Spark + Apache Iceberg 연동
- S3 호환 오브젝트 스토리지로서의 MinIO
- Iceberg 테이블 작업 (생성, 읽기, 쓰기, 스키마 변경, 타임 트래블 등)

---

## Spark 3.4 + MinIO + Kubernetes 연결 테스트

Spark 3.4 (Scala 2.12) 로 MinIO(S3-호환) 에 Parquet / CSV 데이터를 쓰고 읽는 테스트입니다.

### 프로젝트 구조

```
.
├── spark-minio-test/               # Scala Spark 프로젝트
│   ├── build.sbt                   # Spark 3.4 / hadoop-aws / aws-sdk 의존성
│   ├── project/
│   │   ├── build.properties        # sbt 1.9.7
│   │   └── plugins.sbt             # sbt-assembly (fat jar)
│   ├── src/main/scala/com/example/
│   │   └── MinIOSparkTest.scala    # 메인 테스트 앱
│   ├── Dockerfile                  # 멀티스테이지 빌드
│   └── entrypoint.sh               # 컨테이너 진입점
└── k8s/                            # Kubernetes 매니페스트
    ├── namespace.yaml              # spark-test 네임스페이스
    ├── minio.yaml                  # MinIO 배포 + 버킷 초기화 Job
    ├── spark-job.yaml              # Spark 테스트 Job (로컬 모드)
    └── spark-submit-on-k8s.yaml    # spark-submit 클러스터 모드 예시
```

### 테스트 시나리오

| 테스트 | 내용 |
|--------|------|
| TEST 1 | Parquet 쓰기 – 직원 샘플 데이터 5건 |
| TEST 2 | Parquet 읽기 및 스키마/데이터 출력 |
| TEST 3 | SQL 집계 (부서별 인원/평균연령) + 결과 저장 |
| TEST 4 | CSV 쓰기 → 읽기 라운드트립 |

### 빠른 시작

#### 1. 이미지 빌드

```bash
cd spark-minio-test
docker build -t your-registry/spark-minio-test:1.0.0 .
docker push your-registry/spark-minio-test:1.0.0
```

#### 2. Kubernetes 배포

```bash
# 네임스페이스 생성
kubectl apply -f k8s/namespace.yaml

# MinIO 배포 + 버킷 초기화
kubectl apply -f k8s/minio.yaml
kubectl wait --for=condition=complete job/minio-init -n spark-test --timeout=120s

# spark-job.yaml 에서 이미지 이름 수정 후 배포
kubectl apply -f k8s/spark-job.yaml

# 로그 확인
kubectl logs -f job/spark-minio-test -n spark-test
```

#### 3. 로컬 Docker 테스트

```bash
# MinIO 실행
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"

# 버킷 생성
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/spark-test

# Spark 테스트 실행
docker run --rm --network host \
  -e MINIO_ENDPOINT=http://localhost:9000 \
  -e MINIO_ACCESS_KEY=minioadmin \
  -e MINIO_SECRET_KEY=minioadmin \
  -e MINIO_BUCKET=spark-test \
  your-registry/spark-minio-test:1.0.0
```

### 환경변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO API 엔드포인트 |
| `MINIO_ACCESS_KEY` | `minioadmin` | 액세스 키 |
| `MINIO_SECRET_KEY` | `minioadmin` | 시크릿 키 |
| `MINIO_BUCKET` | `spark-test` | 테스트에 사용할 버킷 |

### 의존성 버전

| 컴포넌트 | 버전 |
|----------|------|
| Spark | 3.4.1 |
| Scala | 2.12.17 |
| hadoop-aws | 3.3.4 |
| aws-java-sdk-bundle | 1.12.262 |
| Java | 17 |
| sbt | 1.9.7 |
