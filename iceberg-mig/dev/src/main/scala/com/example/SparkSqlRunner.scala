package com.example

import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * SparkSqlRunner — Spark SQL 실행기
 *
 * 사용법:
 *   spark-submit --class com.example.SparkSqlRunner dev-1.0.0-exec.jar [옵션]
 *
 * 옵션 (환경변수 또는 인수로 전달):
 *   --sql  "<SQL 문>"      : 인라인 SQL 직접 실행
 *   --file "<경로>.sql"    : SQL 파일 경로 지정 (여러 문장 세미콜론 구분)
 *   --db   "<database>"   : 사용할 데이터베이스 (기본: default)
 *
 * MinIO / S3 접속 환경변수:
 *   MINIO_ENDPOINT    (기본: http://minio:9000)
 *   MINIO_ACCESS_KEY  (기본: minioadmin)
 *   MINIO_SECRET_KEY  (기본: minioadmin)
 *   MINIO_BUCKET      (기본: spark-test)
 *
 * Iceberg 카탈로그 환경변수:
 *   ICEBERG_CATALOG   (기본: local)  — "local" | "hadoop" | "hive" | "rest"
 *   ICEBERG_WAREHOUSE (기본: s3a://<MINIO_BUCKET>/warehouse)
 */
object SparkSqlRunner {

  // ── 환경변수 읽기 ─────────────────────────────────────────────────────────
  private val minioEndpoint  = sys.env.getOrElse("MINIO_ENDPOINT",    "http://minio:9000")
  private val minioAccessKey = sys.env.getOrElse("MINIO_ACCESS_KEY",  "minioadmin")
  private val minioSecretKey = sys.env.getOrElse("MINIO_SECRET_KEY",  "minioadmin")
  private val minioBucket    = sys.env.getOrElse("MINIO_BUCKET",      "spark-test")
  private val icebergCatalog = sys.env.getOrElse("ICEBERG_CATALOG",   "local")
  private val warehousePath  = sys.env.getOrElse("ICEBERG_WAREHOUSE",  s"s3a://$minioBucket/warehouse")

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)

    val sqlStatements: Seq[String] = params.get("--sql") match {
      case Some(inlineSql) =>
        splitStatements(inlineSql)
      case None =>
        params.get("--file") match {
          case Some(filePath) => loadSqlFile(filePath)
          case None =>
            println(
              """[ERROR] SQL이 지정되지 않았습니다.
                |  --sql  "<SQL>"        : 인라인 SQL
                |  --file "<path>.sql"   : SQL 파일
                |""".stripMargin)
            sys.exit(1)
        }
    }

    val database = params.getOrElse("--db", "default")

    val spark = buildSparkSession()

    try {
      spark.sql(s"USE $database")
      println(s"[INFO] 데이터베이스: $database")
      println(s"[INFO] SQL 문장 수 : ${sqlStatements.size}\n")

      sqlStatements.zipWithIndex.foreach { case (sql, idx) =>
        runSingleSql(spark, sql.trim, idx + 1)
      }

      println("\n[SUCCESS] 모든 SQL 실행 완료")
    } catch {
      case e: Exception =>
        println(s"\n[FAILED] 실행 실패: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }

  // ── SparkSession 빌드 ────────────────────────────────────────────────────
  private def buildSparkSession(): SparkSession = {
    val builder = SparkSession.builder()
      .appName("SparkSqlRunner")
      .master("local[*]")
      // ── S3A / MinIO ──────────────────────────────────────────────────────
      .config("spark.hadoop.fs.s3a.endpoint",                  minioEndpoint)
      .config("spark.hadoop.fs.s3a.access.key",                minioAccessKey)
      .config("spark.hadoop.fs.s3a.secret.key",                minioSecretKey)
      .config("spark.hadoop.fs.s3a.path.style.access",         "true")
      .config("spark.hadoop.fs.s3a.impl",                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.multipart.size",            "104857600")
      .config("spark.hadoop.fs.s3a.connection.maximum",        "100")
      .config("spark.hadoop.fs.s3a.attempts.maximum",          "3")
      // ── Iceberg 카탈로그 ─────────────────────────────────────────────────
      .config("spark.sql.extensions",
              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config(s"spark.sql.catalog.$icebergCatalog",
              "org.apache.iceberg.spark.SparkCatalog")
      .config(s"spark.sql.catalog.$icebergCatalog.type",       "hadoop")
      .config(s"spark.sql.catalog.$icebergCatalog.warehouse",  warehousePath)
      .config("spark.sql.defaultCatalog",                       icebergCatalog)
      // ── 기타 ─────────────────────────────────────────────────────────────
      .config("spark.sql.shuffle.partitions",                  "4")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  // ── SQL 1문장 실행 ────────────────────────────────────────────────────────
  private def runSingleSql(spark: SparkSession, sql: String, seq: Int): Unit = {
    if (sql.isEmpty) return

    println(s"── SQL #$seq ${"─" * 50}")
    println(sql)
    println()

    Try(spark.sql(sql)) match {
      case Success(df) =>
        // SELECT 계열은 결과를 출력, DDL/DML은 행 수 없을 수 있음
        if (df.columns.nonEmpty) {
          df.show(truncate = false)
          println(s"[OK] #$seq 완료 (${df.count()} rows)\n")
        } else {
          println(s"[OK] #$seq 완료\n")
        }
      case Failure(e) =>
        throw new RuntimeException(s"SQL #$seq 실패: ${e.getMessage}", e)
    }
  }

  // ── 유틸: 인수 파싱 ──────────────────────────────────────────────────────
  private def parseArgs(args: Array[String]): Map[String, String] = {
    val pairs = args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") => key -> value
    }
    pairs.toMap
  }

  // ── 유틸: SQL 파일 로드 ───────────────────────────────────────────────────
  private def loadSqlFile(path: String): Seq[String] = {
    val content = Try(new String(Files.readAllBytes(Paths.get(path)), "UTF-8"))
      .getOrElse {
        // 클래스패스에서도 시도
        val src = Source.fromResource(path)
        try src.mkString finally src.close()
      }
    splitStatements(content)
  }

  // ── 유틸: 세미콜론으로 SQL 분리 (주석 포함 처리) ─────────────────────────
  private def splitStatements(content: String): Seq[String] =
    content
      .split(";")
      .map(_.trim)
      .filter(_.nonEmpty)
      .filterNot(_.startsWith("--"))
      .toSeq
}
