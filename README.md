# iceberg_test

**Apache Spark 3.4 + Apache Iceberg + MinIO** 연동을 검증하는 테스트 프로젝트입니다.
S3 호환 오브젝트 스토리지인 MinIO를 백엔드로 사용하며, Kubernetes 환경에서 동작을 확인합니다.

---

## 기술 스택

| 컴포넌트 | 버전 |
|----------|------|
| Apache Spark | 3.4.1 |
| Scala | 2.12.17 |
| Java (OpenJDK) | 17 |
| sbt | 1.9.7 |
| hadoop-aws | 3.3.4 |
| aws-java-sdk-bundle | 1.12.262 |

---

## 프로젝트 구조

```
.
├── spark-minio-test/                    # Scala Spark 프로젝트
│   ├── build.sbt                        # 의존성 정의 (Spark / hadoop-aws / aws-sdk)
│   ├── project/
│   │   ├── build.properties             # sbt 1.9.7
│   │   └── plugins.sbt                  # sbt-assembly (fat jar 빌드)
│   ├── src/main/scala/com/example/
│   │   └── MinIOSparkTest.scala         # 메인 테스트 애플리케이션
│   ├── Dockerfile                       # 멀티스테이지 빌드 (builder → spark runtime)
│   └── entrypoint.sh                    # 컨테이너 진입점 (spark-submit 실행)
└── k8s/                                 # Kubernetes 매니페스트
    ├── namespace.yaml                   # spark-test 네임스페이스
    ├── minio.yaml                       # MinIO Deployment + Service + 버킷 초기화 Job
    ├── spark-job.yaml                   # Spark 테스트 Job (로컬 모드, RBAC 포함)
    └── spark-submit-on-k8s.yaml        # spark-submit 클러스터 모드 예시
```

---

## 테스트 시나리오

`MinIOSparkTest.scala`에서 아래 4가지 테스트를 순차 실행합니다.

| 순서 | 함수 | 내용 |
|------|------|------|
| 1 | `testWriteParquet` | 직원 샘플 데이터 5건을 Parquet 형식으로 MinIO에 쓰기 |
| 2 | `testReadParquet` | 저장된 Parquet 파일 읽기 및 스키마·데이터 출력 |
| 3 | `testAggregation` | 부서별 인원·평균연령 집계 SQL 실행 후 결과 저장 |
| 4 | `testCsvRoundtrip` | CSV 쓰기 → 읽기 라운드트립 검증 |

모든 테스트 통과 시 `[SUCCESS] 모든 테스트 통과!` 출력, 실패 시 `System.exit(1)`.

---

## 빠른 시작

### 옵션 A — 로컬 Docker

```bash
# 1. MinIO 컨테이너 실행
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"

# 2. 버킷 생성
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/spark-test

# 3. 이미지 빌드 (spark-minio-test/ 디렉터리에서 실행)
cd spark-minio-test
docker build -t spark-minio-test:1.0.0 .

# 4. 테스트 실행
docker run --rm --network host \
  -e MINIO_ENDPOINT=http://localhost:9000 \
  -e MINIO_ACCESS_KEY=minioadmin \
  -e MINIO_SECRET_KEY=minioadmin \
  -e MINIO_BUCKET=spark-test \
  spark-minio-test:1.0.0
```

---

### 옵션 B — Kubernetes

```bash
# 1. 이미지 빌드 및 레지스트리 푸시
cd spark-minio-test
docker build -t <your-registry>/spark-minio-test:1.0.0 .
docker push <your-registry>/spark-minio-test:1.0.0

# 2. 네임스페이스 생성
kubectl apply -f k8s/namespace.yaml

# 3. MinIO 배포 + 버킷 초기화 완료 대기
kubectl apply -f k8s/minio.yaml
kubectl wait --for=condition=complete job/minio-init \
  -n spark-test --timeout=120s

# 4. spark-job.yaml의 image 필드를 실제 이미지로 수정 후 배포
#    image: <your-registry>/spark-minio-test:1.0.0
kubectl apply -f k8s/spark-job.yaml

# 5. 로그 확인
kubectl logs -f job/spark-minio-test -n spark-test
```

> **클러스터 모드(spark-submit)** 를 사용하려면 `k8s/spark-submit-on-k8s.yaml`을 참고하세요.

---

## 환경변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO API 엔드포인트 |
| `MINIO_ACCESS_KEY` | `minioadmin` | 액세스 키 |
| `MINIO_SECRET_KEY` | `minioadmin` | 시크릿 키 |
| `MINIO_BUCKET` | `spark-test` | 테스트용 버킷 이름 |

---

## Dockerfile 빌드 구조

```
Stage 1 (builder)   sbtscala/scala-sbt:eclipse-temurin-17.0.5_8_1.9.7_2.12.17
  └─ sbt assembly → spark-minio-test-1.0.0.jar (fat jar)

Stage 2 (runtime)   apache/spark:3.4.1-scala2.12-java17-python3-ubuntu
  ├─ hadoop-aws-3.3.4.jar         (Maven Central에서 다운로드)
  ├─ aws-java-sdk-bundle-1.12.262.jar
  └─ spark-minio-test-1.0.0.jar  (Stage 1에서 복사)
```

---

## S3A 주요 Spark 설정

| 설정 키 | 값 | 설명 |
|---------|----|------|
| `fs.s3a.endpoint` | `$MINIO_ENDPOINT` | MinIO 주소 |
| `fs.s3a.path.style.access` | `true` | Path-style URL (MinIO 필수) |
| `fs.s3a.impl` | `S3AFileSystem` | S3A 파일시스템 구현체 |
| `fs.s3a.multipart.size` | `104857600` (100 MB) | 멀티파트 업로드 청크 크기 |
| `fs.s3a.connection.maximum` | `100` | 최대 동시 연결 수 |
| `fs.s3a.attempts.maximum` | `3` | 재시도 횟수 |
