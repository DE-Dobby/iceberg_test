package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Spark 3.4 + MinIO (S3A) 연결 테스트
 *
 * 환경변수로 MinIO 접속 정보를 받아 아래 시나리오를 순차 실행:
 *  1. MinIO 연결 확인
 *  2. 샘플 데이터 Parquet 으로 MinIO 에 쓰기
 *  3. 쓴 데이터 다시 읽기
 *  4. 간단한 집계 쿼리
 *  5. CSV 읽기/쓰기
 */
object MinIOSparkTest {

  // ── 환경변수 기본값 ──────────────────────────────────────────────────────────
  private val minioEndpoint  = sys.env.getOrElse("MINIO_ENDPOINT",   "http://minio:9000")
  private val minioAccessKey = sys.env.getOrElse("MINIO_ACCESS_KEY", "minioadmin")
  private val minioSecretKey = sys.env.getOrElse("MINIO_SECRET_KEY", "minioadmin")
  private val minioBucket    = sys.env.getOrElse("MINIO_BUCKET",     "spark-test")
  private val basePath       = s"s3a://$minioBucket"

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("  Spark 3.4 + MinIO Connection Test")
    println("=" * 60)
    println(s"  Endpoint  : $minioEndpoint")
    println(s"  Bucket    : $minioBucket")
    println("=" * 60)

    val spark = buildSparkSession()

    try {
      testWriteParquet(spark)
      testReadParquet(spark)
      testAggregation(spark)
      testCsvRoundtrip(spark)

      println("\n[SUCCESS] 모든 테스트 통과!")
    } catch {
      case e: Exception =>
        println(s"\n[FAILED] 테스트 실패: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  // ── SparkSession 빌드 ────────────────────────────────────────────────────────
  private def buildSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Spark-MinIO-Test")
      // 로컬 모드 (Kubernetes Job 단독 실행용); 클러스터 제출 시 .master() 제거
      .master("local[*]")
      // S3A MinIO 설정
      .config("spark.hadoop.fs.s3a.endpoint",                 minioEndpoint)
      .config("spark.hadoop.fs.s3a.access.key",               minioAccessKey)
      .config("spark.hadoop.fs.s3a.secret.key",               minioSecretKey)
      .config("spark.hadoop.fs.s3a.path.style.access",        "true")
      .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      // 멀티파트 업로드 / 연결 안정성
      .config("spark.hadoop.fs.s3a.multipart.size",           "104857600")  // 100MB
      .config("spark.hadoop.fs.s3a.connection.maximum",       "100")
      .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
      .config("spark.hadoop.fs.s3a.connection.timeout",       "200000")
      // Kubernetes 환경에서 DNS 캐시 문제 방지
      .config("spark.hadoop.fs.s3a.attempts.maximum",         "3")
      .config("spark.hadoop.fs.s3a.retry.interval.ms",        "500")
      // 불필요한 로그 줄이기
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  // ── 테스트 1: Parquet 쓰기 ───────────────────────────────────────────────────
  private def testWriteParquet(spark: SparkSession): Unit = {
    println("\n[TEST 1] Parquet 쓰기...")

    import spark.implicits._

    val df = Seq(
      (1, "Alice",   29, "Engineering"),
      (2, "Bob",     34, "Marketing"),
      (3, "Charlie", 23, "Engineering"),
      (4, "Diana",   41, "HR"),
      (5, "Eve",     28, "Engineering")
    ).toDF("id", "name", "age", "dept")

    val path = s"$basePath/parquet/employees"
    df.write.mode("overwrite").parquet(path)
    println(s"  -> 완료: $path (rows=${df.count()})")
  }

  // ── 테스트 2: Parquet 읽기 ───────────────────────────────────────────────────
  private def testReadParquet(spark: SparkSession): Unit = {
    println("\n[TEST 2] Parquet 읽기...")

    val path = s"$basePath/parquet/employees"
    val df   = spark.read.parquet(path)

    df.printSchema()
    df.show(truncate = false)

    val count = df.count()
    assert(count == 5, s"row count 불일치: 기대=5 실제=$count")
    println(s"  -> 완료: rows=$count")
  }

  // ── 테스트 3: 집계 쿼리 ─────────────────────────────────────────────────────
  private def testAggregation(spark: SparkSession): Unit = {
    println("\n[TEST 3] 집계 쿼리...")

    val path = s"$basePath/parquet/employees"
    val df   = spark.read.parquet(path)

    df.createOrReplaceTempView("employees")

    val result = spark.sql(
      """
        |SELECT dept,
        |       COUNT(*)        AS cnt,
        |       AVG(age)        AS avg_age,
        |       MAX(age)        AS max_age,
        |       MIN(age)        AS min_age
        |FROM employees
        |GROUP BY dept
        |ORDER BY dept
        |""".stripMargin
    )

    result.show(truncate = false)

    val aggPath = s"$basePath/parquet/dept_summary"
    result.write.mode("overwrite").parquet(aggPath)
    println(s"  -> 집계 결과 저장: $aggPath")
  }

  // ── 테스트 4: CSV 라운드트립 ─────────────────────────────────────────────────
  private def testCsvRoundtrip(spark: SparkSession): Unit = {
    println("\n[TEST 4] CSV 라운드트립...")

    import spark.implicits._

    val data = Seq(
      ("2024-01-01", "productA", 100.0, 3),
      ("2024-01-01", "productB",  50.5, 7),
      ("2024-01-02", "productA", 100.0, 5),
      ("2024-01-02", "productC", 200.0, 2)
    ).toDF("date", "product", "price", "qty")

    val csvPath = s"$basePath/csv/sales"
    data.write
      .option("header", "true")
      .mode("overwrite")
      .csv(csvPath)
    println(s"  -> CSV 쓰기 완료: $csvPath")

    val schema = StructType(Seq(
      StructField("date",    StringType,  nullable = true),
      StructField("product", StringType,  nullable = true),
      StructField("price",   DoubleType,  nullable = true),
      StructField("qty",     IntegerType, nullable = true)
    ))

    val readDf = spark.read
      .option("header", "true")
      .schema(schema)
      .csv(csvPath)

    readDf.show(truncate = false)

    val revenue = readDf.withColumn("revenue", col("price") * col("qty"))
    revenue.show(truncate = false)

    val count = readDf.count()
    assert(count == 4, s"CSV row count 불일치: 기대=4 실제=$count")
    println(s"  -> 완료: rows=$count")
  }
}
