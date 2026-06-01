et -euo pipefail

#  Configuration
PROJECT_DIR="/home/maria_dev/nasdaq-volatility-pipeline"
PYTHON="python3.8"
HDFS_ROOT="/user/maria_dev/nasdaq"
HIVE_DB="nasdaq"
INCREMENTAL=false
LOG_DIR="${PROJECT_DIR}/logs"
LOG_FILE="${LOG_DIR}/pipeline_$(date +%Y%m%d_%H%M%S).log"

# Colour helpers
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

info()    { echo -e "${GREEN}[INFO]${NC}  $(date '+%Y-%m-%d %H:%M:%S')  $*" | tee -a "$LOG_FILE"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $(date '+%Y-%m-%d %H:%M:%S')  $*" | tee -a "$LOG_FILE"; }
error()   { echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S')  $*" | tee -a "$LOG_FILE" >&2; }
section() {
    echo -e "\n${BLUE}══════════════════════════════════════════════════════${NC}" | tee -a "$LOG_FILE"
    echo -e "${BLUE}  $*${NC}" | tee -a "$LOG_FILE"
    echo -e "${BLUE}══════════════════════════════════════════════════════${NC}" | tee -a "$LOG_FILE"
}

# Parse arguments 
for arg in "$@"; do
    case $arg in
        --incremental) INCREMENTAL=true ;;
        *) warn "Unknown argument: $arg" ;;
    esac
done

#  Setup 
mkdir -p "$LOG_DIR"
mkdir -p "${PROJECT_DIR}/data/raw"
mkdir -p "${PROJECT_DIR}/visualization/outputs"

cd "$PROJECT_DIR"

PIPELINE_START=$(date +%s)

echo "" | tee -a "$LOG_FILE"
section "NASDAQ-100 Volatility Pipeline"
info "Mode        : $([ "$INCREMENTAL" = true ] && echo 'INCREMENTAL' || echo 'FULL')"
info "Project dir : $PROJECT_DIR"
info "Log file    : $LOG_FILE"
info "Python      : $($PYTHON --version 2>&1)"

#  Stage 1: Data Collection
section "Stage 1 - Data Collection"

STAGE_START=$(date +%s)

if [ "$INCREMENTAL" = true ]; then
    info "Running incremental ingestion (latest trading day)..."
    $PYTHON ingestion/ingest_ohlcv.py --incremental
else
    info "Running full ingestion (5 years)..."
    $PYTHON ingestion/ingest_ohlcv.py
fi

STAGE_END=$(date +%s)
CSV_COUNT=$(find data/raw -name "*.csv" ! -name "nasdaq100_meta.csv" | wc -l)
DATA_SIZE=$(du -sh data/raw/ | cut -f1)
info "Stage 1 complete in $((STAGE_END - STAGE_START))s"
info "CSV files   : $CSV_COUNT tickers"
info "Data size   : $DATA_SIZE"

#  Stage 2: HDFS Upload 
section "Stage 2 - HDFS Upload"

STAGE_START=$(date +%s)

if [ "$INCREMENTAL" = true ]; then
    info "Uploading new files to HDFS (incremental)..."
    bash hdfs/upload_to_hdfs.sh --incremental
else
    info "Uploading all files to HDFS (full)..."
    bash hdfs/upload_to_hdfs.sh
fi

STAGE_END=$(date +%s)
HDFS_SIZE=$(hdfs dfs -du -s -h "${HDFS_ROOT}/raw/csv" 2>/dev/null | awk '{print $1}' || echo "unknown")
info "Stage 2 complete in $((STAGE_END - STAGE_START))s"
info "HDFS raw size : $HDFS_SIZE"

# Stage 3: Spark Preprocessing 
section "Stage 3 - Spark Preprocessing"

STAGE_START=$(date +%s)
info "Submitting Spark job..."

spark-submit \
    --master "local[*]" \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.catalogImplementation=in-memory \
    spark/preprocess.py \
        --input-hdfs  "${HDFS_ROOT}/raw/csv" \
        --meta-hdfs   "${HDFS_ROOT}/raw/meta/nasdaq100_meta.csv" \
        --output-hdfs "${HDFS_ROOT}/processed"

STAGE_END=$(date +%s)
PARQUET_SIZE=$(hdfs dfs -du -s -h "${HDFS_ROOT}/processed" 2>/dev/null | awk '{print $1}' || echo "unknown")
info "Stage 3 complete in $((STAGE_END - STAGE_START))s"
info "Parquet size : $PARQUET_SIZE"

#  Stage 4: Hive Analysis
section "Stage 4 - Hive Analysis"

STAGE_START=$(date +%s)

# Refresh Hive table partitions
info "Refreshing Hive table..."
hive -e "USE ${HIVE_DB}; MSCK REPAIR TABLE stock_features;" 2>/dev/null || \
    warn "Hive repair failed — table may need to be recreated with: hive -f hive/create_tables.hql"

# Run analysis queries and save results
info "Running monthly volatility query..."
hive -f hive/monthly_volatility.hql > "${LOG_DIR}/monthly_volatility_$(date +%Y%m%d).txt" 2>/dev/null
info "  Saved -> ${LOG_DIR}/monthly_volatility_$(date +%Y%m%d).txt"

info "Running sector risk query..."
hive -f hive/sector_risk.hql > "${LOG_DIR}/sector_risk_$(date +%Y%m%d).txt" 2>/dev/null
info "  Saved -> ${LOG_DIR}/sector_risk_$(date +%Y%m%d).txt"

info "Running outlier detection query..."
hive -f hive/outlier_detection.hql > "${LOG_DIR}/outlier_detection_$(date +%Y%m%d).txt" 2>/dev/null
info "  Saved -> ${LOG_DIR}/outlier_detection_$(date +%Y%m%d).txt"

STAGE_END=$(date +%s)
info "Stage 4 complete in $((STAGE_END - STAGE_START))s"

# Stage 5: Visualization 
section "Stage 5 - Visualization"

STAGE_START=$(date +%s)

info "Generating volatility heatmap..."
$PYTHON visualization/volatility_heatmap.py
info "  Saved -> visualization/outputs/volatility_heatmap.png"

info "Generating sector bar chart..."
$PYTHON visualization/sector_bar_chart.py
info "  Saved -> visualization/outputs/sector_bar_chart.png"

info "Generating time-series plot..."
$PYTHON visualization/volatility_timeseries.py
info "  Saved -> visualization/outputs/volatility_timeseries.png"

# Upload charts to HDFS for access via Ambari
info "Uploading charts to HDFS..."
hdfs dfs -put -f visualization/outputs/volatility_heatmap.png "${HDFS_ROOT}/" 2>/dev/null || true
hdfs dfs -put -f visualization/outputs/sector_bar_chart.png "${HDFS_ROOT}/" 2>/dev/null || true
hdfs dfs -put -f visualization/outputs/volatility_timeseries.png "${HDFS_ROOT}/" 2>/dev/null || true

STAGE_END=$(date +%s)
info "Stage 5 complete in $((STAGE_END - STAGE_START))s"

#  Final Summary
PIPELINE_END=$(date +%s)
TOTAL=$((PIPELINE_END - PIPELINE_START))

echo "" | tee -a "$LOG_FILE"
echo "════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
echo "  PIPELINE COMPLETE" | tee -a "$LOG_FILE"
echo "════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
echo "  Mode        : $([ "$INCREMENTAL" = true ] && echo 'INCREMENTAL' || echo 'FULL')" | tee -a "$LOG_FILE"
echo "  Total time  : ${TOTAL}s ($(( TOTAL / 60 ))m $(( TOTAL % 60 ))s)" | tee -a "$LOG_FILE"
echo "  CSV files   : $CSV_COUNT tickers" | tee -a "$LOG_FILE"
echo "  HDFS raw    : $HDFS_SIZE" | tee -a "$LOG_FILE"
echo "  Parquet     : $PARQUET_SIZE" | tee -a "$LOG_FILE"
echo "  Log file    : $LOG_FILE" | tee -a "$LOG_FILE"
echo "  Charts      : visualization/outputs/" | tee -a "$LOG_FILE"
echo "  Analysis    : ${LOG_DIR}/" | tee -a "$LOG_FILE"
echo "════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
