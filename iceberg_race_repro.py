"""
Iceberg V1 포맷 — 버킷 데이터 전체 유실 버그 재현
====================================================
테이블: format-version = 1, bucket(4, bkt) 파티셔닝

재현 목표:
  특정 버킷의 레코드가 0 건이 되거나, MERGE 도중
  FileNotFoundException 이 발생해 커밋 결과가 불완전해지는 현상.

핵심 원리 (왜 V1 에서 더 취약한가):
  V1 은 delete 파일이 없다. MERGE 는 무조건 copy-on-write:
    1) 스냅샷 S 의 버킷 파일 목록을 scan plan 에 고정
    2) 해당 파일들을 읽어 새 파일 M0..M3 를 작성
    3) "이전 파일 → M0..M3" 로 교체 커밋

  만약 step 1~2 사이에 다른 잡이 파일을 교체(overwrite)하고
  expire_snapshots 이 S 의 파일들을 물리 삭제하면:
    → step 2 에서 FileNotFoundException (파일이 없으니 읽기 실패)
    → 또는 step 3 커밋 시 삭제된 파일을 "이전 파일" 로 지정해
      새 스냅샷에서 일부 버킷이 통째로 누락 (0 건)

트리거 조건:
  - AQE 비활성화: spark.sql.adaptive.enabled=false
    (AQE 의 RuntimeFilter 체크가 안전망 역할 — 이것을 제거해야 진짜 파일 접근 시도)
  - INGEST 를 overwrite 방식으로: 기존 파일이 즉시 expire 대상으로 전환
  - expire retain_last=1: 직전 파일까지 즉시 삭제

판단 기준:
  [완전 재현]  FileNotFoundException / NoSuchFileException
               버킷별 레코드 0 건
               전체 레코드 수 < 초기 적재량
  [부분 재현]  ValidationException / IllegalStateException (동시성 충돌 감지)
"""

import os
import sys
import shutil
import time
import threading
import traceback
from datetime import datetime, timezone, timedelta

# ── 경로 ───────────────────────────────────────────────────────────────────────
ICEBERG_JAR  = "/tmp/iceberg_jars/iceberg-spark-runtime-3.4_2.12-1.8.0.jar"
WAREHOUSE    = "/tmp/iceberg_repro"
TABLE_FQN    = "local.db.race_tbl"

# ── 파라미터 ───────────────────────────────────────────────────────────────────
NUM_BUCKETS          = 4
INITIAL_RECORDS      = 6_000   # 초기 레코드 (MERGE scan 을 충분히 느리게)
INITIAL_SNAPSHOTS    = 12
CONCURRENT_SNAPSHOTS = 8       # overwrite 스냅샷 수 (많을수록 파일 교체 빠름)
EXPIRE_RETAIN_LAST   = 1       # ★ 핵심: 최신 스냅샷 1개만 유지 → 직전 파일 즉시 삭제
EXPIRE_OLDER_THAN_S  = 1

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
        .appName("iceberg-v1-bucket-loss-repro")
        .master("local[4]")
        .config("spark.jars", ICEBERG_JAR)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type",      "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.local.gc-enabled", "true")
        # ★ AQE 비활성화 — SparkCopyOnWriteScan 의 RuntimeFilter 안전 체크 제거
        #   이것 없이는 "Runtime file filtering is not possible" 에러로 조기 차단됨
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism",    "4")
        .getOrCreate()
    )


def setup_table(spark):
    """
    테이블 초기화 + scan 대상 파일 생성 절차:
      1) append 12회 → S1..S12  (파일 F1..F12 누적)
      2) 첫 MERGE(COW) → S13    (M0..M3 생성, F1..F12 는 S13 에서 제외)
         → expire 가 F1..F12 를 삭제 가능한 상태

    동시 실행 단계의 두 번째 MERGE 는 M0..M3 (S13 의 파일) 를 scan 대상으로 잡음.
    INGEST(overwrite) 가 M0..M3 를 교체하고 expire(retain_last=1) 가 M0..M3 를
    물리 삭제하면, MERGE 가 M0..M3 를 읽으려 할 때 FileNotFoundException 발생.
    """
    spark.sql(f"DROP TABLE IF EXISTS {TABLE_FQN} PURGE")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

    spark.sql(f"""
        CREATE TABLE {TABLE_FQN} (
            id   BIGINT,
            bkt  INT,
            val  STRING,
            ts   TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (bucket({NUM_BUCKETS}, bkt))
        TBLPROPERTIES (
            'format-version'           = '1',
            'commit.retry.num-retries' = '0'
        )
    """)
    print(f"[SETUP] 테이블 생성 (format-version=1): {TABLE_FQN}")

    per_batch = INITIAL_RECORDS // INITIAL_SNAPSHOTS
    for i in range(INITIAL_SNAPSHOTS):
        s = i * per_batch
        spark.sql(f"""
            INSERT INTO {TABLE_FQN}
            SELECT id,
                   CAST(id % {NUM_BUCKETS} AS INT)    AS bkt,
                   CONCAT('init_', CAST(id AS STRING)) AS val,
                   current_timestamp()                AS ts
            FROM (SELECT explode(sequence({s}, {s + per_batch - 1})) AS id)
        """)
        time.sleep(0.04)
    print(f"[SETUP] append {INITIAL_SNAPSHOTS}회 완료")

    # 첫 번째 MERGE(COW): F1..F12 → M0..M3 교체
    # M0..M3 가 이후 두 번째 MERGE 의 scan 대상이 됨
    print("[SETUP] 첫 번째 MERGE(COW) 실행 중...")
    spark.sql(f"""
        MERGE INTO {TABLE_FQN} AS t
        USING (
            SELECT id,
                   CAST(id % {NUM_BUCKETS} AS INT)      AS bkt,
                   CONCAT('v2_', CAST(id AS STRING))    AS val,
                   current_timestamp()                  AS ts
            FROM (SELECT explode(sequence(0, {INITIAL_RECORDS - 1})) AS id)
        ) AS s
        ON t.id = s.id AND t.bkt = s.bkt
        WHEN MATCHED     THEN UPDATE SET t.val = s.val, t.ts = s.ts
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE_FQN}").collect()[0]["cnt"]
    print(f"[SETUP] 완료 — 스냅샷 {INITIAL_SNAPSHOTS + 1}개, 레코드 {count:,}건")
    print(f"[SETUP] 현재 버킷 파일(M0..M3)이 두 번째 MERGE 의 scan 대상")
    return count


def worker_merge(spark, barrier):
    """두 번째 MERGE(COW): M0..M3 를 scan — 이 파일들이 삭제되면 FileNotFoundException"""
    try:
        barrier.wait()
        print("[MERGE] 시작 (scan 대상: M0..M3)")
        spark.sql(f"""
            MERGE INTO {TABLE_FQN} AS t
            USING (
                SELECT id,
                       CAST(id % {NUM_BUCKETS} AS INT)        AS bkt,
                       CONCAT('merged_', CAST(id AS STRING))  AS val,
                       current_timestamp()                    AS ts
                FROM (SELECT explode(sequence(0, {INITIAL_RECORDS - 1})) AS id)
            ) AS s
            ON t.id = s.id AND t.bkt = s.bkt
            WHEN MATCHED     THEN UPDATE SET t.val = s.val, t.ts = s.ts
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("[MERGE] 완료")
    except Exception as e:
        msg = str(e)
        results["merge_error"] = msg
        if "FileNotFoundException" in msg or "NoSuchFileException" in msg \
                or "No such file" in msg:
            results["file_not_found"] = True
            print(f"[MERGE] ★ FileNotFoundException 발생 — 버그 재현!")
        else:
            print(f"[MERGE] 에러: {msg[:200]}")


def worker_ingest(spark, barrier):
    """
    ★ 핵심: overwritePartitions() 로 전체 버킷을 교체
    매 회 실행마다 M0..M3 (또는 이전 회의 파일) 가 새 파일로 교체되어
    expire 삭제 대상이 됨.
    """
    try:
        barrier.wait()
        print("[INGEST] 시작 (overwrite 방식)")
        for i in range(CONCURRENT_SNAPSHOTS):
            s = INITIAL_RECORDS + i * 300
            (spark.range(s, s + 300)
                  .selectExpr(
                      "id",
                      f"CAST(id % {NUM_BUCKETS} AS INT) AS bkt",
                      "CONCAT('ow_', CAST(id AS STRING))  AS val",
                      "current_timestamp() AS ts",
                  )
                  .writeTo(TABLE_FQN)
                  .overwritePartitions())   # dynamic partition overwrite: 버킷 파일 교체
            time.sleep(0.02)
        print(f"[INGEST] 완료 — overwrite {CONCURRENT_SNAPSHOTS}회")
    except Exception as e:
        results["ingest_error"] = str(e)
        print(f"[INGEST] 에러: {str(e)[:200]}")


def worker_expire(spark, barrier):
    """
    retain_last=1: 최신 스냅샷 1개만 남기고 나머지 + 파일 즉시 삭제.
    INGEST 가 overwrite 로 M0..M3 를 교체한 직후 실행되면
    M0..M3 는 어떤 live 스냅샷도 참조하지 않으므로 물리 삭제됨.
    """
    try:
        barrier.wait()
        time.sleep(0.15)   # MERGE 가 scan plan 을 잡은 뒤 expire 실행
        print("[EXPIRE] 시작 (retain_last=1)")
        older_than = datetime.now(timezone.utc) - timedelta(seconds=EXPIRE_OLDER_THAN_S)
        older_than_str = older_than.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        row = spark.sql(f"""
            CALL local.system.expire_snapshots(
                table       => '{TABLE_FQN}',
                older_than  => TIMESTAMP '{older_than_str}',
                retain_last => {EXPIRE_RETAIN_LAST}
            )
        """).collect()[0]

        deleted_data  = row["deleted_data_files_count"]
        deleted_mfest = row["deleted_manifest_files_count"]
        deleted_ml    = row["deleted_manifest_lists_count"]
        results["expire_deleted_data_files"] = deleted_data
        print(
            f"[EXPIRE] 완료 — 삭제: data_files={deleted_data}, "
            f"manifests={deleted_mfest}, manifest_lists={deleted_ml}"
        )
        if deleted_data > 0:
            print(f"[EXPIRE] ★ 데이터 파일 {deleted_data}개 물리 삭제 확인!")
    except Exception as e:
        results["expire_error"] = str(e)
        print(f"[EXPIRE] 에러: {str(e)[:200]}")


def run_concurrent(spark):
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
    print("\n" + "=" * 60)
    print("  결과 검증")
    print("=" * 60)

    try:
        after_count = spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {TABLE_FQN}"
        ).collect()[0]["cnt"]
        results["after_count"] = after_count
    except Exception as e:
        after_count = -1
        results["after_count"] = str(e)
        if "FileNotFoundException" in str(e) or "NoSuchFileException" in str(e) \
                or "No such file" in str(e):
            results["file_not_found"] = True
        print(f"  [전체 카운트 실패] {str(e)[:300]}")

    print(f"  BEFORE : {before_count:,}건")
    if isinstance(after_count, int) and after_count >= 0:
        diff = after_count - before_count
        sign = "+" if diff >= 0 else ""
        print(f"  AFTER  : {after_count:,}건  ({sign}{diff})")
    else:
        print(f"  AFTER  : 조회 실패")

    print("\n  [버킷별 레코드 수]")
    try:
        rows = spark.sql(f"""
            SELECT bkt, COUNT(*) AS cnt
            FROM {TABLE_FQN}
            GROUP BY bkt ORDER BY bkt
        """).collect()
        print(f"  {'bkt':>4}  {'cnt':>8}")
        print(f"  {'-'*4}  {'-'*8}")
        for r in rows:
            flag = "  ← ★ 0건!" if r["cnt"] == 0 else ""
            print(f"  {r['bkt']:>4}  {r['cnt']:>8}{flag}")
            results["bucket_counts"][r["bkt"]] = r["cnt"]
    except Exception as e:
        if "FileNotFoundException" in str(e) or "NoSuchFileException" in str(e) \
                or "No such file" in str(e):
            results["file_not_found"] = True
        print(f"  버킷 조회 실패: {str(e)[:200]}")

    print("\n  [스냅샷 히스토리 (최근 10개)]")
    try:
        spark.sql(f"""
            SELECT snapshot_id, committed_at, operation
            FROM {TABLE_FQN}.snapshots
            ORDER BY committed_at DESC LIMIT 10
        """).show(truncate=False)
    except Exception as e:
        print(f"  스냅샷 조회 실패: {e}")

    # expire 실제 삭제 파일 수
    if results["expire_deleted_data_files"] > 0:
        print(f"  [EXPIRE] 물리 삭제된 data_files = {results['expire_deleted_data_files']}")

    for key, label in [("merge_error", "MERGE"), ("ingest_error", "INGEST"),
                       ("expire_error", "EXPIRE")]:
        if results[key]:
            # 핵심 에러 메시지만 요약해서 출력
            msg = results[key]
            first_line = msg.split('\n')[0]
            print(f"\n  [{label} 에러 요약] {first_line[:300]}")


def judge():
    print("\n" + "=" * 60)
    print("  최종 판단")
    print("=" * 60)

    full_repro    = False
    partial_repro = False
    reasons_full  = []
    reasons_part  = []

    # ── 완전 재현 기준 ────────────────────────────────────────────────────────
    if results["file_not_found"]:
        full_repro = True
        reasons_full.append("FileNotFoundException / NoSuchFileException 발생")

    zero_bkts = [k for k, v in results["bucket_counts"].items() if v == 0]
    if zero_bkts:
        full_repro = True
        reasons_full.append(f"레코드 0건 버킷 발견: {zero_bkts}")

    after = results["after_count"]
    if isinstance(after, int) and after >= 0 and after < INITIAL_RECORDS:
        full_repro = True
        reasons_full.append(
            f"레코드 수 감소: {results['before_count']:,} → {after:,} "
            f"(초기 {INITIAL_RECORDS:,}건 미달)"
        )

    merge_err = results["merge_error"] or ""
    if "FileNotFoundException" in merge_err or "NoSuchFileException" in merge_err \
            or "No such file" in merge_err:
        full_repro = True
        reasons_full.append("MERGE 에러에서 FileNotFoundException 확인")

    # ── 부분 재현 기준 ────────────────────────────────────────────────────────
    if "ValidationException" in merge_err and "conflicting" in merge_err:
        partial_repro = True
        reasons_part.append("MERGE ValidationException: 동시성 충돌 감지 (OCC)")

    if "Runtime file filtering is not possible" in merge_err or \
       "concurrently modified" in merge_err:
        partial_repro = True
        reasons_part.append(
            "MERGE IllegalStateException: scan snapshot ≠ current snapshot"
        )

    if results["expire_deleted_data_files"] > 0:
        partial_repro = True
        reasons_part.append(
            f"expire 가 data_files {results['expire_deleted_data_files']}개 물리 삭제 — "
            "타이밍이 맞으면 MERGE FileNotFoundException 으로 직결"
        )

    # ── 출력 ─────────────────────────────────────────────────────────────────
    if full_repro:
        print("  ★★ 완전 재현 성공 ★★")
        for r in reasons_full:
            print(f"    - {r}")
        if reasons_part:
            print("  (추가 관찰)")
            for r in reasons_part:
                print(f"    - {r}")
    elif partial_repro:
        print("  △ 부분 재현 — 레이스 컨디션 발생 확인 (완전한 데이터 유실은 이번 실행에서 미발생)")
        for r in reasons_part:
            print(f"    - {r}")
        print()
        print("  완전 재현(FileNotFoundException / 0건 버킷)을 위한 조건:")
        print("    1. expire_deleted_data_files > 0 이면서 MERGE 가 해당 파일을 읽는 타이밍")
        print("    2. CONCURRENT_SNAPSHOTS 늘리기 (더 빠른 파일 교체)")
        print("    3. 반복 실행 — 레이스 컨디션은 확률적")
    else:
        print("  ○ 미발생 — 이번 실행에서는 레이스 컨디션 없음")
        print("    → CONCURRENT_SNAPSHOTS 증가, INITIAL_RECORDS 증가, 반복 실행 권장")

    print("=" * 60)
    return full_repro or partial_repro


def main():
    if os.path.exists(WAREHOUSE):
        shutil.rmtree(WAREHOUSE)
    os.makedirs(WAREHOUSE, exist_ok=True)

    print("=" * 60)
    print("  Iceberg V1 bucket 데이터 유실 버그 재현 테스트")
    print(f"  format-version=1 / AQE=OFF / expire retain_last={EXPIRE_RETAIN_LAST}")
    print("=" * 60)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        before_count = setup_table(spark)

        # format-version 확인
        try:
            row = spark.sql(
                f"SHOW TBLPROPERTIES {TABLE_FQN} ('format-version')"
            ).collect()
            print(f"[INFO] format-version = {row[0][1] if row else '?'}")
        except Exception:
            pass

        print(f"\n[RACE] 동시 실행 시작 (MERGE + overwrite-INGEST × {CONCURRENT_SNAPSHOTS}"
              f" + expire retain_last={EXPIRE_RETAIN_LAST})")
        t0 = time.time()
        run_concurrent(spark)
        print(f"[RACE] 완료 — {time.time() - t0:.1f}s")

        verify_results(spark, before_count)
        reproduced = judge()
        return 1 if reproduced else 0

    except Exception as e:
        print(f"\n[FATAL] {e}")
        traceback.print_exc()
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
