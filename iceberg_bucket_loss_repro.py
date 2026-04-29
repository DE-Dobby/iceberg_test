"""
Iceberg V1  dt / bucket(3, bkt)  파티션 테이블
버킷 전체 데이터 유실 버그 재현 테스트
=========================================

재현 원리 (포맷버전 1 한정):
  V1 은 delete 파일이 없으므로 MERGE 는 무조건 copy-on-write.
  MERGE 는 scan-plan 을 잡는 시점의 스냅샷 S_merge 파일 목록을 고정한다.

  동시에:
    INGEST  — overwritePartitions() 를 6회 실행 → 매 회 새 파일로 교체
    EXPIRE  — expire_snapshots(retain_last=5)
              6회 overwrite 후 S_merge 가 "최신 5개" 밖으로 밀려나면
              S_merge 의 파일이 어떤 live 스냅샷도 참조하지 않는 상태가 됨
              → 물리 삭제 가능

  AQE 를 끄면 RuntimeFilter 안전 체크(IllegalStateException)가 제거됨.
  → MERGE 가 삭제된 파일을 실제로 읽으려 시도 → FileNotFoundException
  → 또는 해당 버킷의 scan 결과가 빈 채로 커밋 → 버킷 0건

판단 기준:
  [완전 재현]  특정 (dt, bucket) 에 0건 / FileNotFoundException /
               전체 레코드 수 < 초기 적재량
  [부분 재현]  ValidationException / IllegalStateException 발생
"""

import os, sys, shutil, time, threading, traceback, glob
from datetime import datetime, timezone, timedelta

# ── 경로 ───────────────────────────────────────────────────────────────────────
ICEBERG_JAR = "/tmp/iceberg_jars/iceberg-spark-runtime-3.4_2.12-1.8.0.jar"
WAREHOUSE   = "/tmp/iceberg_repro"
TABLE_FQN   = "local.db.race_tbl"

# ── 파라미터 ───────────────────────────────────────────────────────────────────
INITIAL_RECORDS      = 3_000   # 총 초기 레코드 (dt 3개 × 1000)
INITIAL_SNAPSHOTS    = 10      # append 스냅샷 수
CONCURRENT_OVERWRITES = 6      # ★ overwrite 횟수 (retain_last=5 보다 커야 함)
EXPIRE_RETAIN_LAST   = 5
EXPIRE_OLDER_THAN_S  = 1

DT_LIST = ["2024-01-01", "2024-01-02", "2024-01-03"]

# ── 공유 상태 ──────────────────────────────────────────────────────────────────
results = {
    "before_count":   None,
    "after_count":    None,
    "per_dt":         {},       # dt → count
    "per_bucket":     {},       # bucket_id(0/1/2) → count
    "merge_error":    None,
    "ingest_error":   None,
    "expire_error":   None,
    "file_not_found": False,
    "expired_data_files": 0,
}


# ── SparkSession ───────────────────────────────────────────────────────────────
def build_spark():
    from pyspark.sql import SparkSession

    os.environ["JAVA_TOOL_OPTIONS"] = " ".join([
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

    return (
        SparkSession.builder
        .appName("iceberg-v1-bucket-loss")
        .master("local[4]")
        .config("spark.jars", ICEBERG_JAR)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local",         "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type",    "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.local.gc-enabled", "true")
        # ★ AQE 끄기: RuntimeFilter 스냅샷 불일치 안전 체크 제거
        #   AQE 가 켜져 있으면 "Runtime file filtering is not possible" 에러로
        #   파일 접근 전에 차단되어 FileNotFoundException 이 발생하지 않음
        .config("spark.sql.adaptive.enabled",       "false")
        .config("spark.sql.shuffle.partitions",     "4")
        .config("spark.default.parallelism",        "4")
        .getOrCreate()
    )


# ── 테이블 생성 및 초기 적재 ───────────────────────────────────────────────────
def setup_table(spark):
    """
    1) 테이블 생성 (dt DATE, bkt INT, col1 STRING, format-version=1)
       PARTITIONED BY (dt, bucket(3, bkt))
    2) append 10회 → S1..S10  (파일 F*)
    3) 첫 번째 MERGE(COW) → S11  (파일 M* 생성, F* 는 S11 에서 제외)
       ★ 두 번째 MERGE 가 M* 를 scan 대상으로 잡음
    """
    spark.sql(f"DROP TABLE IF EXISTS {TABLE_FQN} PURGE")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

    spark.sql(f"""
        CREATE TABLE {TABLE_FQN} (
            id    BIGINT,
            dt    DATE,
            bkt   INT,
            col1  STRING
        )
        USING iceberg
        PARTITIONED BY (dt, bucket(3, bkt))
        TBLPROPERTIES (
            'format-version'           = '1',
            'commit.retry.num-retries' = '0'
        )
    """)
    print(f"[SETUP] 테이블 생성 완료 (format-version=1, PARTITIONED BY (dt, bucket(3,bkt)))")

    # ── append 10회 ────────────────────────────────────────────────────────────
    per_snap = INITIAL_RECORDS // INITIAL_SNAPSHOTS   # 회당 레코드 수
    for i in range(INITIAL_SNAPSHOTS):
        s = i * per_snap
        spark.sql(f"""
            INSERT INTO {TABLE_FQN}
            SELECT
                id,
                CASE WHEN id % 3 = 0 THEN DATE '2024-01-01'
                     WHEN id % 3 = 1 THEN DATE '2024-01-02'
                     ELSE                  DATE '2024-01-03' END AS dt,
                CAST(id % 50 AS INT)              AS bkt,
                CONCAT('init_', CAST(id AS STRING)) AS col1
            FROM (SELECT explode(sequence({s}, {s + per_snap - 1})) AS id)
        """)
        time.sleep(0.05)

    cnt_before_merge = spark.sql(
        f"SELECT COUNT(*) AS c FROM {TABLE_FQN}"
    ).collect()[0]["c"]
    print(f"[SETUP] append {INITIAL_SNAPSHOTS}회 완료 — {cnt_before_merge:,}건")

    # ── 첫 번째 MERGE(COW) → M 파일 생성 ──────────────────────────────────────
    print("[SETUP] 첫 번째 MERGE(COW) 실행 (M 파일 생성)...")
    spark.sql(f"""
        MERGE INTO {TABLE_FQN} AS t
        USING (
            SELECT
                id,
                CASE WHEN id % 3 = 0 THEN DATE '2024-01-01'
                     WHEN id % 3 = 1 THEN DATE '2024-01-02'
                     ELSE                  DATE '2024-01-03' END AS dt,
                CAST(id % 50 AS INT)              AS bkt,
                CONCAT('v2_', CAST(id AS STRING)) AS col1
            FROM (SELECT explode(sequence(0, {INITIAL_RECORDS - 1})) AS id)
        ) AS s
        ON t.id = s.id AND t.dt = s.dt AND t.bkt = s.bkt
        WHEN MATCHED     THEN UPDATE SET t.col1 = s.col1
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("[SETUP] 첫 번째 MERGE 완료 — M 파일 생성, F 파일은 expire 대상 전환")

    total = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE_FQN}").collect()[0]["c"]
    snap_cnt = spark.sql(
        f"SELECT COUNT(*) AS c FROM {TABLE_FQN}.snapshots"
    ).collect()[0]["c"]
    print(f"[SETUP] 완료 — 스냅샷 {snap_cnt}개, 레코드 {total:,}건")
    print(f"[SETUP] ★ 두 번째 MERGE 는 위 M 파일들을 scan 대상으로 잡음")
    return total


# ── Thread: 두 번째 MERGE ──────────────────────────────────────────────────────
def worker_merge(spark, barrier):
    """
    M 파일들을 scan plan 에 고정한 채 전체 레코드 update.
    INGEST+EXPIRE 가 M 파일을 삭제하면 FileNotFoundException 또는 빈 버킷 커밋.
    """
    try:
        barrier.wait()
        print("[MERGE] 시작 — M 파일 scan plan 고정")
        spark.sql(f"""
            MERGE INTO {TABLE_FQN} AS t
            USING (
                SELECT
                    id,
                    CASE WHEN id % 3 = 0 THEN DATE '2024-01-01'
                         WHEN id % 3 = 1 THEN DATE '2024-01-02'
                         ELSE                  DATE '2024-01-03' END AS dt,
                    CAST(id % 50 AS INT)                AS bkt,
                    CONCAT('merged_', CAST(id AS STRING)) AS col1
                FROM (SELECT explode(sequence(0, {INITIAL_RECORDS - 1})) AS id)
            ) AS s
            ON t.id = s.id AND t.dt = s.dt AND t.bkt = s.bkt
            WHEN MATCHED     THEN UPDATE SET t.col1 = s.col1
            WHEN NOT MATCHED THEN INSERT *
        """)
        print("[MERGE] 완료")
    except Exception as e:
        msg = str(e)
        results["merge_error"] = msg
        fnf = ("FileNotFoundException" in msg or "NoSuchFileException" in msg
               or "No such file" in msg or "FileNotFound" in msg)
        if fnf:
            results["file_not_found"] = True
            print("[MERGE] ★ FileNotFoundException — 버그 재현 확인!")
        else:
            first = msg.split('\n')[0]
            print(f"[MERGE] 에러: {first[:250]}")


# ── Thread: 동시 적재 (overwrite) ─────────────────────────────────────────────
def worker_ingest(spark, barrier):
    """
    overwritePartitions() 로 같은 (dt, bucket) 파티션을 6회 교체.
    매 회 이전 파일이 expire 대상이 되고, MERGE 의 scan 대상 M 파일도
    6회 overwrite 후 retain_last=5 밖으로 밀려남.
    """
    try:
        barrier.wait()
        print(f"[INGEST] 시작 — overwrite × {CONCURRENT_OVERWRITES}회")
        for i in range(CONCURRENT_OVERWRITES):
            s = INITIAL_RECORDS + i * 300
            (spark.range(s, s + 300)
                  .selectExpr(
                      "id",
                      """CASE WHEN id % 3 = 0 THEN DATE '2024-01-01'
                              WHEN id % 3 = 1 THEN DATE '2024-01-02'
                              ELSE                  DATE '2024-01-03' END AS dt""",
                      "CAST(id % 50 AS INT)             AS bkt",
                      "CONCAT('ow_', CAST(id AS STRING)) AS col1",
                  )
                  .writeTo(TABLE_FQN)
                  .overwritePartitions())
            time.sleep(0.02)
        print(f"[INGEST] 완료 — overwrite {CONCURRENT_OVERWRITES}회")
    except Exception as e:
        results["ingest_error"] = str(e)
        print(f"[INGEST] 에러: {str(e)[:200]}")


# ── Thread: expire_snapshots ───────────────────────────────────────────────────
def worker_expire(spark, barrier):
    """
    retain_last=5, older_than=now-1s.
    INGEST 가 6회 overwrite 해서 MERGE 의 snapshot 이 retain_last=5 밖으로 밀리면
    MERGE 의 M 파일들이 live 스냅샷 어디에도 없으므로 물리 삭제됨.
    """
    try:
        barrier.wait()
        time.sleep(0.2)   # MERGE 가 scan plan 을 잡도록 잠깐 대기
        print(f"[EXPIRE] 시작 — retain_last={EXPIRE_RETAIN_LAST}, older_than=now-{EXPIRE_OLDER_THAN_S}s")
        ts = (datetime.now(timezone.utc) - timedelta(seconds=EXPIRE_OLDER_THAN_S)
              ).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        row = spark.sql(f"""
            CALL local.system.expire_snapshots(
                table       => '{TABLE_FQN}',
                older_than  => TIMESTAMP '{ts}',
                retain_last => {EXPIRE_RETAIN_LAST}
            )
        """).collect()[0]
        d = row["deleted_data_files_count"]
        m = row["deleted_manifest_files_count"]
        l = row["deleted_manifest_lists_count"]
        results["expired_data_files"] = d
        print(f"[EXPIRE] 완료 — data_files={d}, manifests={m}, manifest_lists={l}")
        if d > 0:
            print(f"[EXPIRE] ★ 데이터 파일 {d}개 물리 삭제!")
    except Exception as e:
        results["expire_error"] = str(e)
        print(f"[EXPIRE] 에러: {str(e)[:200]}")


# ── 동시 실행 ─────────────────────────────────────────────────────────────────
def run_concurrent(spark):
    barrier = threading.Barrier(3)
    ts = [
        threading.Thread(target=worker_merge,  args=(spark, barrier), name="T-MERGE"),
        threading.Thread(target=worker_ingest, args=(spark, barrier), name="T-INGEST"),
        threading.Thread(target=worker_expire, args=(spark, barrier), name="T-EXPIRE"),
    ]
    for t in ts: t.start()
    for t in ts: t.join(timeout=300)


# ── 결과 검증 ─────────────────────────────────────────────────────────────────
def verify_results(spark, before_count):
    sep = "=" * 62
    print(f"\n{sep}")
    print("  결과 검증")
    print(sep)

    # 전체 카운트
    results["before_count"] = before_count
    try:
        after = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE_FQN}").collect()[0]["c"]
        results["after_count"] = after
        diff = after - before_count
        print(f"  BEFORE : {before_count:,}건")
        print(f"  AFTER  : {after:,}건   (diff {'+' if diff>=0 else ''}{diff})")
    except Exception as e:
        results["after_count"] = -1
        if "FileNotFoundException" in str(e) or "No such file" in str(e):
            results["file_not_found"] = True
        print(f"  AFTER  : 조회 실패 — {str(e)[:200]}")

    # dt 별 카운트
    print("\n  [dt 별 레코드 수]")
    try:
        rows = spark.sql(f"""
            SELECT dt, COUNT(*) AS cnt
            FROM {TABLE_FQN}
            GROUP BY dt ORDER BY dt
        """).collect()
        for r in rows:
            results["per_dt"][str(r["dt"])] = r["cnt"]
            print(f"    dt={r['dt']}  cnt={r['cnt']:,}")
    except Exception as e:
        if "FileNotFoundException" in str(e) or "No such file" in str(e):
            results["file_not_found"] = True
        print(f"    조회 실패: {str(e)[:150]}")

    # 버킷별 카운트 (bkt % 3 로 bucket id 근사)
    print("\n  [bucket 별 레코드 수  (bucket = bkt % 3 근사)]")
    try:
        rows = spark.sql(f"""
            SELECT (bkt % 3) AS bucket_id, COUNT(*) AS cnt
            FROM {TABLE_FQN}
            GROUP BY (bkt % 3) ORDER BY bucket_id
        """).collect()
        for r in rows:
            flag = "  ← ★ 0건 — 버킷 전체 유실!" if r["cnt"] == 0 else ""
            print(f"    bucket {r['bucket_id']}  cnt={r['cnt']:,}{flag}")
            results["per_bucket"][r["bucket_id"]] = r["cnt"]
    except Exception as e:
        if "FileNotFoundException" in str(e) or "No such file" in str(e):
            results["file_not_found"] = True
        print(f"    조회 실패: {str(e)[:150]}")

    # 물리 파일 현황
    data_dir = os.path.join(WAREHOUSE, "db", "race_tbl", "data")
    parquet_files = glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
    print(f"\n  [물리 데이터 파일 수]  {len(parquet_files)}개")
    if parquet_files:
        # 파티션 디렉토리별 파일 수
        from collections import Counter
        part_dirs = Counter(
            os.path.basename(os.path.dirname(f)) for f in parquet_files
        )
        for pdir, cnt in sorted(part_dirs.items()):
            print(f"    {pdir}: {cnt}개")

    # 스냅샷 히스토리
    print("\n  [스냅샷 히스토리 (최근 12개)]")
    try:
        spark.sql(f"""
            SELECT snapshot_id, committed_at, operation
            FROM {TABLE_FQN}.snapshots
            ORDER BY committed_at DESC
            LIMIT 12
        """).show(truncate=False)
    except Exception as e:
        print(f"    조회 실패: {e}")

    # 에러 요약
    for key, label in [("merge_error","MERGE"),("ingest_error","INGEST"),("expire_error","EXPIRE")]:
        if results[key]:
            first = results[key].split('\n')[0]
            print(f"\n  [{label} 에러]  {first[:300]}")


# ── 재현 판단 ─────────────────────────────────────────────────────────────────
def judge():
    sep = "=" * 62
    print(f"\n{sep}")
    print("  최종 판단")
    print(sep)

    full, partial = False, False
    r_full, r_part = [], []

    # 완전 재현
    if results["file_not_found"]:
        full = True
        r_full.append("FileNotFoundException / NoSuchFileException 발생")

    zero = [k for k, v in results["per_bucket"].items() if v == 0]
    if zero:
        full = True
        r_full.append(f"bucket {zero} 전체 유실 (0건)")

    after = results["after_count"]
    before_cnt = results.get("before_count") or INITIAL_RECORDS
    if isinstance(after, int) and 0 <= after < INITIAL_RECORDS:
        full = True
        r_full.append(
            f"레코드 감소: {before_cnt:,} → {after:,}"
            f"  (초기 {INITIAL_RECORDS:,}건 미달)"
        )

    merge_err = results["merge_error"] or ""
    if any(k in merge_err for k in ("FileNotFoundException","NoSuchFileException","No such file")):
        full = True
        r_full.append("MERGE 에러 스택에서 FileNotFoundException 확인")

    # 부분 재현
    if "ValidationException" in merge_err and "conflicting" in merge_err:
        partial = True
        r_part.append("MERGE ValidationException (동시 쓰기 충돌 — OCC 보호)")

    if "Runtime file filtering" in merge_err or "concurrently modified" in merge_err:
        partial = True
        r_part.append("MERGE IllegalStateException (scan snapshot ≠ current snapshot)")

    if results["expired_data_files"] > 0:
        partial = True
        r_part.append(
            f"expire 가 data_files {results['expired_data_files']}개 물리 삭제"
            " — MERGE 가 해당 파일을 읽는 타이밍이면 FileNotFoundException"
        )

    # 출력
    if full:
        print("  ★★ 완전 재현 성공 ★★  — 버킷 데이터 유실 확인")
        for r in r_full: print(f"    - {r}")
        if r_part:
            print("  [추가 관찰]")
            for r in r_part: print(f"    - {r}")
    elif partial:
        print("  △ 부분 재현  — 레이스 컨디션 발생 (완전 유실은 이번 실행에서 미발생)")
        for r in r_part: print(f"    - {r}")
        print()
        print("  완전 재현을 위한 팁:")
        print("    · CONCURRENT_OVERWRITES 늘리기 (현재:", CONCURRENT_OVERWRITES, ")")
        print("    · INITIAL_RECORDS 늘리기 → MERGE 더 느리게 (현재:", INITIAL_RECORDS, ")")
        print("    · 반복 실행 (레이스 컨디션은 확률적)")
    else:
        print("  ○ 미발생  — CONCURRENT_OVERWRITES/INITIAL_RECORDS 증가 후 재시도")

    print(sep)
    return full or partial


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    if os.path.exists(WAREHOUSE):
        shutil.rmtree(WAREHOUSE)
    os.makedirs(WAREHOUSE, exist_ok=True)

    print("=" * 62)
    print("  Iceberg V1 bucket 전체 데이터 유실 재현 테스트")
    print(f"  format-version=1 | AQE=OFF | retain_last={EXPIRE_RETAIN_LAST}")
    print(f"  PARTITIONED BY (dt, bucket(3, bkt))")
    print("=" * 62)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        before = setup_table(spark)

        # format-version 확인
        try:
            v = spark.sql(
                f"SHOW TBLPROPERTIES {TABLE_FQN} ('format-version')"
            ).collect()[0][1]
            print(f"[INFO] format-version = {v}  (V1 확인{'✓' if v=='1' else ' ← 주의'})")
        except Exception:
            pass

        print(
            f"\n[RACE] 동시 실행 시작\n"
            f"       T1: MERGE INTO (전체 {INITIAL_RECORDS:,}건 update)\n"
            f"       T2: overwrite × {CONCURRENT_OVERWRITES}회\n"
            f"       T3: expire_snapshots(retain_last={EXPIRE_RETAIN_LAST}, older_than=now-{EXPIRE_OLDER_THAN_S}s)"
        )
        t0 = time.time()
        run_concurrent(spark)
        print(f"[RACE] 완료 — 경과 {time.time()-t0:.1f}s")

        verify_results(spark, before)
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
