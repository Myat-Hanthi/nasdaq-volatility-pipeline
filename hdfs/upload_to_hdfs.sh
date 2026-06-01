#1.Create HDFS directory structure under /user/maria_dev/nasdaq/
#2.Upload raw CSVs from data/raw/ into HDFS
#3.Converts CSVs -> parquet via a PySpark job
#4. Verifies the upload and prints a size report

#bash hdfs/upload_to_hdfs.sh full upload
#bash hdfs/upload_to_hdfs.sh --incremental upload only files newer than previous run

set -euo pipefail

#configuration

HDFS_USER="maria_dev"
LOCAL_RAW_DIR="data/raw"
HDFS_ROOT="/user/${HDFS_USER}/nasdaq"
HDFS_RAW_CSV="${HDFS_ROOT}/raw/csv"
HDFS_RAW_META="${HDFS_ROOT}/raw/meta"
HDFS_PROCESSED="${HDFS_ROOT}/processed"
SPARK_JOB="spark/preprocess.py"
INCREMENTAL=false
TIMESTAMP_FILE=".last_upload_ts"

#colour helpers
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }
section() { echo -e "\n${BLUE}══════════════════════════════════════${NC}"; \
            echo -e "${BLUE}  $*${NC}"; \
            echo -e "${BLUE}══════════════════════════════════════${NC}"; }

#parse arguments
for arg in "$@"; do
    case $arg in
        --incremental) INCREMENTAL=true;;
        --user=*)      HDFS_USER="${arg#*=}"
                       HDFS_ROOT= "/user/${HDFS_USER}/nasdaq"
                       HDFS_RAW_CSV="${HDFS_ROOT}/raw/csv"
                       HDFS_RAW_META="${HDFS_ROOT}/raw/meta"
                       HDFS_PROCESSED="${HDFS_ROOT}/processed"
                       ;;
        *) warn "Unknown argument: $arg" ;;
    esac
done

#Pre-flight checks
section "Pre-flight checks"

#confirm running as correct user
CURRENT_USER=$(whoami)
info "Running as user : $CURRENT_USER"
info "HDFS target user : $HDFS_USER"

if [ "$CURRENT_USER" != "$HDFS_USER" ]; then
    warn "You are logged in as '$CURRENT_USER' but HDFS_USER is '$HDFS_USER'."
    warn "HDFS writes will go to /user/${HDFS_USER}/nasdaq/."
    warn "If you see permission errors, switch user:  su - ${HDFS_USER}"
fi

#check hdfs command
if ! command -v hdfs &>/dev/null; then
    error "'hdfs' command not found."
    error "Make sure you are on the HDP Sandbox and Hadoop is in PATH."
    error "Try: export PATH=\$PATH:/usr/hdp/current/hadoop-client/bin"
    exit 1
fi

#check spark-submit
if ! command -v spark-submit &>/dev/null; then
    error "'spark-submit' not found."
    error "Try: export PATH=\$PATH:/usr/hdp/current/spark2-client/bin"
    exit 1
fi

# Check local raw data exists
if [ ! -d "$LOCAL_RAW_DIR" ]; then
    error "Local raw data directory '$LOCAL_RAW_DIR' not found."
    error "Run this from the project root and make sure Stage 1 has been run:"
    error "  python ingestion/ingest_ohlcv.py"
    exit 1
fi

CSV_COUNT=$(find "$LOCAL_RAW_DIR" -name "*.csv" ! -name "nasdaq100_meta.csv" | wc -l)
if [ "$CSV_COUNT" -eq 0 ]; then
    error "No ticker CSV files found in '$LOCAL_RAW_DIR'."
    error "Run: python ingestion/ingest_ohlcv.py"
    exit 1
fi

info "Local CSV files  : $CSV_COUNT tickers"
info "Incremental mode : $INCREMENTAL"
info "HDFS root        : $HDFS_ROOT"

#1. Create HDFS directory structure
section "Step-1 - Creating HDFS directories"

for dir in "$HDFS_RAW_CSV" "$HDFS_RAW_META" "$HDFS_PROCESSED"; do
    if hdfs dfs -test -d "$dir" 2>/dev/null; then
        info "Already exists : $dir"
    else
        hdfs dfs -mkdir -p "$dir"
        info "Created        : $dir"
    fi
done

#Grant write permission to maria_dev (safe to run even if already set)
hdfs dfs -chmod -R 755 "$HDFS_ROOT"
info "Permission set    : 755 on $HDFS_ROOT"

#2. Upload metadata csv
section "Step 2 - Uploading metadata"

META_FILE="${LOCAL_RAW_DIR}/nasdaq100_meta.csv"
if [ -f "$META_FILE" ]; then
    hdfs dfs -put -f "$META_FILE" "$HDFS_RAW_META/"
    META_ROWS=$(wc -l < "$META_FILE")
    info "Uploaded : nasdaq100_meta.csv  (${META_ROWS} lines)  →  $HDFS_RAW_META/"
else
    warn "nasdaq100_meta.csv not found — skipping."
    warn "Run: python ingestion/ingest_ohlcv.py"
fi

#3.Upload ticker CSVs
section "Step 3 - Uploading ticker CSVs"

UPLOADED=0
FAILED=0

#Incremental mode: only upload files modified since the last run
if  "$INCREMENTAL" = true ] && [ -f "$TIMESTAMP_FILE" ]; then
    info "Incremental — uploading only files newer than last run"
    FIND_ARGS=(-newer "$TIMESTAMP_FILE")
else
    info "Full upload — uploading all $CSV_COUNT ticker CSVs"
    FIND_ARGS=""
fi

while IFS= read -r csv_file; do
    ticker=$(basename "$csv_file" .csv)

    if hdfs dfs -put -f "$csv_file" "$HDFS_RAW_CSV/" 2>/dev/null; then
        ROWS=$(wc -l < "$csv_file")
        SIZE=$(du -sh "$csv_file" | cut -f1)
        info "  ✓ ${ticker}  ${ROWS} rows  ${SIZE}"
        UPLOADED=$((UPLOADED + 1))
    else
        error "  ✗ Failed: $csv_file"
        FAILED=$((FAILED + 1))
    fi
 
done < <(find "$LOCAL_RAW_DIR" -name "*.csv" \
              ! -name "nasdaq100_meta.csv" \
              ${FIND_ARGS} \
              | sort)
 
info "Result — uploaded: $UPLOADED  failed: $FAILED"
 
if [ "$FAILED" -gt 0 ]; then
    error "$FAILED files failed. Check HDFS service in Ambari."
    exit 1
fi

#Save timestamp for next incremental run
date -Iseconds > "$TIMESTAMP_FILE"

#4.Convert CSV -> Parquet(Spark)
section "Step 4 - CSV to Parquet (Spark)"

if [ ! -f "$SPARK_JOB" ]; then
    warn "Spark job '$SPARK_JOB' not found — skipping Parquet conversion."
    warn "This step runs automatically once Stage 3 is built."
else
    info "Submitting: spark-submit $SPARK_JOB"
    info "Input  : $HDFS_RAW_CSV"
    info "Output : $HDFS_PROCESSED"

    spark-submit \
        --master local[*] \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf spark.sql.shuffle.partitions=4 \
        --conf "spark.hadoop.fs.defaultFS=hdfs://sandbox-hdp.hortonworks.com:8020" \
        "$SPARK_JOB" \
            --input-hdfs  "$HDFS_RAW_CSV" \
            --meta-hdfs   "$HDFS_RAW_META/nasdaq100_meta.csv" \
            --output-hdfs "$HDFS_PROCESSED"

    info "Spark job done → $HDFS_PROCESSED"
fi

#5.Verification report

echo ""
echo "  HDFS sizes:"
echo "  ──────────────────────────────────────────────────"
 
for dir in "$HDFS_RAW_CSV" "$HDFS_RAW_META" "$HDFS_PROCESSED"; do
    SIZE=$(hdfs dfs -du -s -h "$dir" 2>/dev/null | awk '{print $1}' || echo "0")
    printf "  %-12s  %s\n" "$SIZE" "$dir"
done
 
echo ""
echo "  File counts:"
echo "  ──────────────────────────────────────────────────"
 
CSV_HDFS=$(hdfs dfs -ls "${HDFS_RAW_CSV}/" 2>/dev/null | grep -c "\.csv" || true)
PARQUET_HDFS=$(hdfs dfs -ls "${HDFS_PROCESSED}/" 2>/dev/null | grep -c "\.parquet" || true)
echo "  Ticker CSVs in HDFS   : $CSV_HDFS"
echo "  Parquet files in HDFS : $PARQUET_HDFS"
 
echo ""
echo "  Sample listing — $HDFS_RAW_CSV:"
echo "  ──────────────────────────────────────────────────"
hdfs dfs -ls "${HDFS_RAW_CSV}/" 2>/dev/null | tail -n +2 | head -6 || true
 
echo ""
echo "════════════════════════════════════════════════════"
echo "  Stage 2 complete ✓"
echo ""
echo "  Raw CSVs  → ${HDFS_RAW_CSV}/"
echo "  Metadata  → ${HDFS_RAW_META}/"
echo "  Processed → ${HDFS_PROCESSED}/"
echo ""
echo "  Next step:"
echo "    spark-submit spark/preprocess.py"
echo "════════════════════════════════════════════════════"
