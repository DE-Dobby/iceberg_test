"""
Iceberg 버그 재현 테스트
========================
MERGE INTO 실행 중 expire_snapshots 가 동시에 동작할 때
MERGE 가 참조하던 파일이 삭제되어 데이터 유실이 발생하는지 확인.

재현 조건:
  - MERGE INTO (전체 레코드 update)  ← 오래 걸리는 쓰기
  - 추가 적재 잡 (스냅샷 6개 빠르게 생성)
  - expire_snapshots(older_than=now-1s, retain_last=5)
  위 3개를 동시에 실행

판단 기준:
  - 버킷별 레코드가 0이 되거나
  - 조회 시 FileNotFoundException 이 발생하면 → 재현 성공
"""

import os
import sys
import shutil
import time
import threading
import traceback
from datetime import datetime, timezone

# ── 경로 설정 ──────────────────────────────────────────────────────────────────
ICEBERG_JAR  = "/tmp/iceberg_jars/iceberg-spark-runtime-3.4_2.12-1.8.0.jar"
WAREHOUSE    = "/tmp/iceberg_repro"
CATALOG_NAME = "local"
TABLE_FQN    = f"{CATALOG_NAME}.db.race_tbl"

# 재현 민감도 조절
NUM_BUCKETS          = 4
INITIAL_RECORDS      = 5_000   # 초기 적재 레코드 수 (MERGE 를 느리게)
INITIAL_SNAPSHOTS    = 12      # 초기 스냅샷 수 (10개 이상)
CONCURRENT_SNAPSHOTS = 6       # 경쟁 잡이 생성할 스냅샷 수
EXPIRE_RETAIN_LAST   = 5
EXPIRE_OLDER_THAN_S  = 3       # 현재 - 3초보다 오래된 것 만료 (공격적)

# ── 공유 상태 ──────────────────────────────────────────────────────────────────
results = {
    "before_count":              None,
    "after_count":               None,
    "bucket_counts":             {},
    "merge_error":               None,
    "ingest_error":              None,
    "expire_error":              None,
    "file_not_found":            False,
    "snapshot_history":          [],
    "expire_deleted_data_files": 0,
}

def build_spark():
    from pyspark.sql import SparkSession

    # Java 17+ 에서 Spark 3.4 가 필요로 하는 반사 접근 허용
    jvm_opts = " ".join([
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ])
    os.environ["JAVA_TOOL_OPTIONS"] = jvm_opts

    return (
        SparkSession.builder
        .appName("iceberg-race-repro")
        .master("local[4]")
        .config("spark.jars", ICEBERG_JAR)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type",       "hadoop")
        .config("spark.sql.catalog.local.warehouse",  WAREHOUSE)
        # 파일 삭제를 즉시 실행 (GC 비활성화 → expire 시 즉각 물리 삭제)
        .config("spark.sql.catalog.local.gc-enabled", "true")
        # 소규모 테스트용 튜닝
        .config("spark.sql.shuffle.partitions",       "4")
        .config("spark.default.parallelism",          "4")
        .config("spark.sql.iceberg.planning.preserve-data-grouping", "false")
        .getOrCreate()
    )


def setup_table(spark):
    """테이블 초기화 + 스냅샷 12개 이상 생성"""
    spark.sql(f"DROP TABLE IF EXISTS {TABLE_FQN} PURGE")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

    spark.sql(f"""
        CREATE TABLE {TABLE_FQN} (
            id     BIGINT,
            bkt    INT,
            val    STRING,
            ts     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (bucket({NUM_BUCKETS}, bkt))
        TBLPROPERTIES (
            'write.delete.mode'       = 'copy-on-write',
            'write.update.mode'       = 'copy-on-write',
            'write.merge.mode'        = 'copy-on-write',
            'commit.retry.num-retries'= '0'
        )
    """)
    print(f"[SETUP] 테이블 생성 완료: {TABLE_FQN}")

    # INITIAL_SNAPSHOTS 개의 스냅샷 생성
    per_batch = INITIAL_RECORDS // INITIAL_SNAPSHOTS
    for i in range(INITIAL_SNAPSHOTS):
        start_id = i * per_batch
        spark.sql(f"""
            INSERT INTO {TABLE_FQN}
            SELECT
                id,
                CAST(id % {NUM_BUCKETS} AS INT)   AS bkt,
                CONCAT('init_', CAST(id AS STRING)) AS val,
                current_timestamp()               AS ts
            FROM (SELECT explode(sequence({start_id}, {start_id + per_batch - 1})) AS id)
        """)
        time.sleep(0.05)  # 스냅샷 타임스탬프 분산

    # 첫 번째 MERGE(COW) 실행 → overwrite 스냅샷 생성
    # 이를 통해 초기 파일들이 "이전 버전"이 되어 expire 대상이 됨
    print("[SETUP] 첫 번째 MERGE(COW) 실행 (파일 교체 유도)...")
    spark.sql(f"""
        MERGE INTO {TABLE_FQN} AS t
        USING (
            SELECT
                id,
                CAST(id % {NUM_BUCKETS} AS INT)       AS bkt,
                CONCAT('v2_', CAST(id AS STRING))     AS val,
                current_timestamp()                   AS ts
            FROM (SELECT explode(sequence(0, {INITIAL_RECORDS - 1})) AS id)
        ) AS s
        ON t.id = s.id AND t.bkt = s.bkt
        WHEN MATCHED THEN UPDATE SET t.val = s.val, t.ts = s.ts
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("[SETUP] 첫 번째 MERGE 완료 — 이전 파일들이 expire 대상으로 전환됨")

    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE_FQN}").collect()[0]["cnt"]
    print(f"[SETUP] 초기 적재 완료 — 스냅샷 {INITIAL_SNAPSHOTS + 1}개, 레코드 {count:,}건")
    return count


def worker_merge(spark, barrier):
    """MERGE INTO: 전체 레코드의 val 컬럼을 업데이트"""
    try:
        barrier.wait()  # 동시 시작
        print("[MERGE] 시작...")
        spark.sql(f"""
            MERGE INTO {TABLE_FQN} AS t
            USING (
                SELECT
                    id,
                    CAST(id % {NUM_BUCKETS} AS INT)    AS bkt,
                    CONCAT('merged_', CAST(id AS STRING)) AS val,
                    current_timestamp()                AS ts
                FROM (SELECT explode(sequence(0, {INITIAL_RECORDS - 1})) AS id)
            ) AS s
            ON t.id = s.id AND t.bkt = s.bkt
            WHEN MATCHED THEN UPDATE SET t.val = s.val, t.ts = s.ts
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("[MERGE] 완료")
    except Exception as e:
        results["merge_error"] = str(e)
        if "FileNotFoundException" in str(e) or "NoSuchFileException" in str(e):
            results["file_not_found"] = True
        print(f"[MERGE] 에러: {e}")


def worker_ingest(spark, barrier):
    """추가 적재: 스냅샷을 CONCURRENT_SNAPSHOTS 개 빠르게 생성"""
    try:
        barrier.wait()  # 동시 시작
        print("[INGEST] 시작...")
        offset = INITIAL_RECORDS
        for i in range(CONCURRENT_SNAPSHOTS):
            start_id = offset + i * 100
            spark.sql(f"""
                INSERT INTO {TABLE_FQN}
                SELECT
                    id,
                    CAST(id % {NUM_BUCKETS} AS INT) AS bkt,
                    CONCAT('extra_', CAST(id AS STRING)) AS val,
                    current_timestamp() AS ts
                FROM (SELECT explode(sequence({start_id}, {start_id + 99})) AS id)
            """)
            time.sleep(0.02)
        print(f"[INGEST] 완료 — 스냅샷 {CONCURRENT_SNAPSHOTS}개 추가")
    except Exception as e:
        results["ingest_error"] = str(e)
        print(f"[INGEST] 에러: {e}")


def worker_expire(spark, barrier):
    """expire_snapshots: now-1초보다 오래된 것, retain_last=5"""
    try:
        barrier.wait()  # MERGE/INGEST와 정확히 동시 시작
        # MERGE 플래닝이 시작할 시간을 살짝 준 뒤 expire 실행
        time.sleep(0.2)
        print("[EXPIRE] 시작...")
        from datetime import datetime, timezone, timedelta
        older_than_dt = datetime.now(timezone.utc) - timedelta(seconds=EXPIRE_OLDER_THAN_S)
        older_than_str = older_than_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        expire_result = spark.sql(f"""
            CALL local.system.expire_snapshots(
                table       => '{TABLE_FQN}',
                older_than  => TIMESTAMP '{older_than_str}',
                retain_last => {EXPIRE_RETAIN_LAST}
            )
        """)
        expire_result.show()
        row = expire_result.collect()[0]
        print(
            f"[EXPIRE] 완료 — "
            f"삭제된 data_files={row['deleted_data_files_count']}, "
            f"manifest_files={row['deleted_manifest_files_count']}, "
            f"manifest_lists={row['deleted_manifest_lists_count']}"
        )
        results["expire_deleted_data_files"] = row["deleted_data_files_count"]
    except Exception as e:
        results["expire_error"] = str(e)
        print(f"[EXPIRE] 에러: {e}")


def run_concurrent(spark):
    """3개 스레드를 Barrier 로 동시 출발"""
    barrier = threading.Barrier(3)
    threads = [
        threading.Thread(target=worker_merge,  args=(spark, barrier), name="T-MERGE"),
        threading.Thread(target=worker_ingest, args=(spark, barrier), name="T-INGEST"),
        threading.Thread(target=worker_expire, args=(spark, barrier), name="T-EXPIRE"),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=300)


def verify_results(spark, before_count):
    """결과 검증 및 보고서 출력"""
    print("\n" + "=" * 60)
    print("  결과 검증")
    print("=" * 60)

    # 전체 카운트
    try:
        after_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE_FQN}").collect()[0]["cnt"]
    except Exception as e:
        after_count = -1
        if "FileNotFoundException" in str(e) or "NoSuchFileException" in str(e):
            results["file_not_found"] = True
        results["after_count"] = str(e)
        print(f"[VERIFY] 전체 카운트 실패: {e}")

    results["before_count"] = before_count
    if results["after_count"] is None:
        results["after_count"] = after_count

    print(f"  BEFORE 레코드 수: {before_count:,}")
    print(f"  AFTER  레코드 수: {after_count:,}" if after_count >= 0 else f"  AFTER  레코드 수: 조회 실패")

    # 버킷별 카운트
    print("\n  [버킷별 레코드 수]")
    try:
        bucket_df = spark.sql(f"""
            SELECT bkt, COUNT(*) AS cnt
            FROM {TABLE_FQN}
            GROUP BY bkt
            ORDER BY bkt
        """)
        bucket_df.show()
        for row in bucket_df.collect():
            results["bucket_counts"][row["bkt"]] = row["cnt"]
    except Exception as e:
        if "FileNotFoundException" in str(e) or "NoSuchFileException" in str(e):
            results["file_not_found"] = True
        print(f"  버킷 조회 실패: {e}")

    # 스냅샷 히스토리
    print("\n  [스냅샷 히스토리 (최근 10개)]")
    try:
        snap_df = spark.sql(f"""
            SELECT snapshot_id, committed_at, operation
            FROM {TABLE_FQN}.snapshots
            ORDER BY committed_at DESC
            LIMIT 10
        """)
        snap_df.show(truncate=False)
        results["snapshot_history"] = [r.asDict() for r in snap_df.collect()]
    except Exception as e:
        print(f"  스냅샷 조회 실패: {e}")

    # 에러 리포트
    for key, label in [("merge_error","MERGE"), ("ingest_error","INGEST"), ("expire_error","EXPIRE")]:
        if results[key]:
            print(f"\n  [{label} 에러]\n  {results[key]}")


def judge():
    """재현 성공/실패 판단

    등급:
      [완전 재현] FileNotFoundException 또는 데이터 유실
      [부분 재현] ValidationException (동시성 충돌 감지 — 파일은 무사하지만
                  expire 타이밍이 맞으면 FileNotFoundException 으로 악화 가능)
      [미발생]    어떤 이상도 없음
    """
    print("\n" + "=" * 60)
    print("  최종 판단")
    print("=" * 60)

    full_repro   = False
    partial_repro = False
    reasons_full   = []
    reasons_partial = []

    # 1. FileNotFoundException → 완전 재현
    if results["file_not_found"]:
        full_repro = True
        reasons_full.append("FileNotFoundException / NoSuchFileException 발생")

    # 2. 버킷 레코드 0 → 완전 재현
    zero_buckets = [k for k, v in results["bucket_counts"].items() if v == 0]
    if zero_buckets:
        full_repro = True
        reasons_full.append(f"레코드 0 버킷 발견: {zero_buckets}")

    # 3. 전체 레코드 감소 → 완전 재현
    after = results["after_count"]
    if isinstance(after, int) and after >= 0:
        if after < INITIAL_RECORDS:
            full_repro = True
            reasons_full.append(
                f"레코드 수 감소: {results['before_count']:,} → {after:,} "
                f"(예상 최소 {INITIAL_RECORDS:,})"
            )

    # 4. MERGE 에러 FileNotFoundException → 완전 재현
    merge_err = results["merge_error"] or ""
    if "FileNotFoundException" in merge_err or "NoSuchFileException" in merge_err:
        full_repro = True
        reasons_full.append("MERGE 에러에서 FileNotFoundException 확인")

    # 5. ValidationException (동시성 충돌) → 부분 재현
    #    Iceberg OCC 가 충돌을 막았지만, expire 타이밍이
    #    commit 전 scan 단계에 맞았다면 FileNotFoundException 으로 악화됨
    if "ValidationException" in merge_err and "conflicting files" in merge_err:
        partial_repro = True
        reasons_partial.append(
            "MERGE ValidationException: 동시 ingest 와 충돌 감지 "
            "(Iceberg OCC 보호 작동, 더 공격적인 expire 타이밍이면 FileNotFoundException 발생 가능)"
        )

    # 6. IllegalStateException "Runtime file filtering is not possible" → 부분 재현
    #    copy-on-write 모드에서 MERGE scan 도중 테이블이 변경됨을 감지
    #    "scan snapshot ID != current snapshot ID" — 정확히 보고된 버그 시나리오
    if "Runtime file filtering is not possible" in merge_err or \
       "the table has been concurrently modified" in merge_err:
        partial_repro = True
        reasons_partial.append(
            "MERGE IllegalStateException: scan 중 테이블 동시 변경 감지 "
            "(SparkCopyOnWriteScan — scan snapshot ID ≠ current snapshot ID)"
        )

    # 6. EXPIRE 가 성공적으로 실행됐는지 확인
    if not results["expire_error"]:
        reasons_partial.append("expire_snapshots 성공 실행 확인")

    if full_repro:
        print("  ★ 완전 재현 성공 ★  — 데이터 유실 / FileNotFoundException 확인")
        for r in reasons_full:
            print(f"    - {r}")
    elif partial_repro:
        print("  △ 부분 재현  — 레이스 컨디션 충돌 감지 (데이터는 보호됨)")
        for r in reasons_partial:
            print(f"    - {r}")
        print()
        print("  완전 재현을 위한 다음 단계:")
        print("    1. EXPIRE_OLDER_THAN_S 를 3~5초로 늘리기")
        print("    2. write.update.mode=copy-on-write 로 변경")
        print("    3. INITIAL_RECORDS 를 5000+ 로 늘려 MERGE 를 더 느리게")
    else:
        print("  ○ 미발생 — 이번 실행에서는 레이스 컨디션 없음")
        print("    재현 확률을 높이려면:")
        print("    - INITIAL_RECORDS, INITIAL_SNAPSHOTS 증가")
        print("    - EXPIRE_OLDER_THAN_S 를 더 크게 (2~5초)")
        print("    - 반복 실행 (레이스 컨디션은 확률적)")
    print("=" * 60)
    return full_repro or partial_repro


def main():
    # 이전 실행 정리
    if os.path.exists(WAREHOUSE):
        shutil.rmtree(WAREHOUSE)
    os.makedirs(WAREHOUSE, exist_ok=True)

    print("=" * 60)
    print("  Iceberg MERGE + expire_snapshots 레이스 컨디션 재현 테스트")
    print(f"  Iceberg 1.8.0 / PySpark 3.4.3")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. 테이블 셋업
        before_count = setup_table(spark)

        # 2. 동시 실행
        print("\n[RACE] 3개 작업 동시 실행 시작...")
        t0 = time.time()
        run_concurrent(spark)
        elapsed = time.time() - t0
        print(f"[RACE] 동시 실행 완료 — {elapsed:.1f}s")

        # 3. 검증
        verify_results(spark, before_count)

        # 4. 판단
        reproduced = judge()
        return 0 if not reproduced else 1

    except Exception as e:
        print(f"\n[FATAL] 예상치 못한 오류: {e}")
        traceback.print_exc()
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
