log()
{
    local log_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    echo "[${log_timestamp}] - $*"
    echo "[${log_timestamp}] - $*" >> "${LOG_FILE}"
}
die()
{
    log "$*"
    exit 2
}