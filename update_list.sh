#!/usr/bin/env bash

export LC_ALL=C
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
set -euo pipefail

# ==============================================================================
# 1. CONSTANTS & DEFAULTS
# ==============================================================================

if [ -t 1 ]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BLUE=''; CYAN=''; NC=''
fi

log_info() { printf "${BLUE}INFO:${NC} %s\n" "$1"; }
log_step() { printf "${CYAN}==>${NC} %s\n" "$1"; }
log_succ() { printf "${GREEN}OK:${NC} %s\n" "$1"; }
log_warn() { printf "${YELLOW}WARN:${NC} %s\n" "$1"; }
log_err()  { printf "${RED}ERR:${NC} %s\n" "$1"; }

DEFAULT_WORKDIR="/tmp/nfqws_updater"
DEFAULT_PAUSE=3
SERVICE_NAME="nfqws-keenetic"
MIN_LINES=10
THRESHOLD_PERCENT=90
DRY_RUN=0
MAX_FILESIZE_BYTES=33554432 # 32 MB
LOG_FILE="/tmp/firewall_update.log"
SKIP_SAFETY=0
NO_RESTART=0

# === Настройки MQTT (Home Assistant) ===
# Фиксируем время старта в формате ISO 8601
START_TS=$(date "+%Y-%m-%dT%H:%M:%S%z")

# ==============================================================================
# 2. EMBEDDED AWK SCRIPTS
# ==============================================================================

AWK_SCRIPT_CLEANER='
    { sub(/#.*/, "") }
    { gsub(/[\x22\[\]{},]/, "") }
    { gsub(/^[ \t]+|[ \t]+$/, "") }
    /^$/ { next }
    /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(\/[0-9]+)?$/ { print > (outdir "/ipv4.raw"); next }
    /:/ && !/[^0-9a-fA-F:.\/]/ { print > (outdir "/ipv6.raw"); next }
'

AWK_SCRIPT_STATS='
    function human(x) { v=(x<0?-x:x); if(v<1024)return x"B"; if(v<1048576)return sprintf("%.1fK",x/1024); return sprintf("%.1fM",x/1048576); }
    BEGIN { diff=curr-last; if(is_bytes)val_str=human(curr);else val_str=curr; delta_str=""; if(last>0&&diff!=0){color=(diff>0)?green:red;sign=(diff>0)?"+":"";d_val=(is_bytes)?human(diff):diff;delta_str=sprintf(" %s(%s%s)%s",color,sign,d_val,nc);} printf "  %-13s: %s%s\n",lbl,val_str,delta_str; }
'

AWK_SCRIPT_OPTIMIZER='
    {
        this_end = $2
        is_covered = 0
        if (last_end != "") {
             if (this_end <= last_end) is_covered = 1
        }
        if (!is_covered) {
            print $3
            last_end = this_end
        }
    }
'

# ==============================================================================
# 3. CONFIGURATION CONTEXT
# ==============================================================================

LIST_FILE=""; EXISTING_FILE=""; OUTPUT_FILE=""; WORKDIR=""; CACHE_DIR=""; TMP_BASE=""
PAUSE=0; CURL_OPTS=""; SORT_RAM=""; SORT_PARALLEL=""; URLS_CONTENT=""; TMPDIR=""; TMP_OUTPUT_FILE=""
STAT_V4=0; STAT_V6=0; STAT_TOTAL=0; STAT_BYTES=0
L_V4=0; L_V6=0; L_TOT=0; L_BYTES=0; L_TS=""; CHANGED=0; SHOW_TS=""
LOCK_FILE=""; LOCK_ID=""

init_configuration() {
    SORT_RAM=$(awk '/MemAvailable/ {
        kb = $2;
        mb = int((kb / 1024) * 0.5);
        if (mb < 64) mb = 64;
        if (mb > 4096) mb = 4096;
        printf "%dM", mb
    }' /proc/meminfo 2>/dev/null || echo "64M")
    
    local cpu_count
    cpu_count=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 1)
    SORT_PARALLEL="$((cpu_count - 1))"
    [ "$SORT_PARALLEL" -lt 1 ] && SORT_PARALLEL=1
    
    OPTIND=1
    local workdir_arg=""; local pause_arg=""
    MQTT_SEND=0
    MQTT_CONF_FILE=""

    # Добавлено двоеточие после M (M:), убрана I
    while getopts ":f:o:e:w:t:SRDM:" opt; do
        case $opt in
            f) LIST_FILE="$OPTARG" ;;
            o) OUTPUT_FILE="$OPTARG" ;;
            e) EXISTING_FILE="$OPTARG" ;;
            w) workdir_arg="$OPTARG" ;;
            t) pause_arg="$OPTARG" ;;
            S) SKIP_SAFETY=1 ;;
            R) NO_RESTART=1 ;;
            D) DRY_RUN=1 ;;
            M) MQTT_SEND=1; MQTT_CONF_FILE="$OPTARG" ;;
            \?) log_err "Invalid option: -$OPTARG"; exit 1 ;;
            :) log_err "Option -$OPTARG requires an argument."; exit 1 ;;
        esac
    done
    
    shift $((OPTIND - 1))
    if [ $# -gt 0 ]; then log_warn "Ignoring extra arguments: $*"; fi
    
    if [ -n "$pause_arg" ] && echo "$pause_arg" | grep -qE '^[0-9]+$'; then 
        PAUSE="$pause_arg"
    else 
        PAUSE="$DEFAULT_PAUSE"
    fi
    
    CURL_OPTS="-sSLfR --connect-timeout 10 --max-time 30 --max-filesize $MAX_FILESIZE_BYTES --retry 3 --retry-delay $PAUSE"
    WORKDIR="${workdir_arg:-$DEFAULT_WORKDIR}"; WORKDIR="${WORKDIR%/}"
    CACHE_DIR="${WORKDIR}/cache"; TMP_BASE="${WORKDIR}/temp"
    if [ -z "$OUTPUT_FILE" ]; then
        local script_dir
        script_dir=$(cd "$(dirname "$0")" && pwd)
        OUTPUT_FILE="${script_dir}/ipset_include.txt"
    fi
    TMP_OUTPUT_FILE="${OUTPUT_FILE}.tmp"
    
    # Генерируем уникальные ID для MQTT на основе имени файла ---
    local base_name
    base_name=$(basename "$OUTPUT_FILE" | sed 's/\.[^.]*$//')
    [ -z "$base_name" ] && base_name="default"
    
    # Заменяем дефисы и спецсимволы на подчеркивания для MQTT ID
    MQTT_SAFE_ID=$(echo "$base_name" | tr -cd 'a-zA-Z0-9' | tr 'A-Z' 'a-z')
    MQTT_DEVICE_NAME="NFQWS Updater ($base_name)"
    
    MQTT_PREFIX="homeassistant/sensor/nfqws_${MQTT_SAFE_ID}"
    MQTT_TOPIC_STATE="${MQTT_PREFIX}/state"

    if [ -n "$LIST_FILE" ] && [ -f "$LIST_FILE" ]; then
        URLS_CONTENT=$(grep -vE '^\s*#|^\s*$' "$LIST_FILE" | tr -d '\r' || true)
    fi

    # Загрузка настроек MQTT из файла
    if [ -n "$MQTT_CONF_FILE" ] && [ -f "$MQTT_CONF_FILE" ]; then
        while IFS='=' read -r key val; do
            # Убираем пробелы и кавычки
            val="${val%\"}"; val="${val#\"}"
            val="${val%\'}"; val="${val#\'}"
            case "$key" in
                MQTT_HOST) MQTT_HOST="$val" ;;
                MQTT_PORT) MQTT_PORT="$val" ;;
                MQTT_USER) MQTT_USER="$val" ;;
                MQTT_PASS) MQTT_PASS="$val" ;;
            esac
        done < <(grep -v '^\s*#' "$MQTT_CONF_FILE" | grep -E '^(MQTT_HOST|MQTT_PORT|MQTT_USER|MQTT_PASS)=')
    else
        log_warn "MQTT config file not found or not specified: $MQTT_CONF_FILE. MQTT disabled."
        MQTT_SEND=0
    fi
}

print_config_table() {
    local safe_str dry_str rest_str
    [ "$SKIP_SAFETY" -eq 1 ] && safe_str="${RED}DISABLED${NC}" || safe_str="${GREEN}ENABLED${NC}"
    [ "$DRY_RUN" -eq 1 ] && dry_str="${GREEN}ENABLED${NC}" || dry_str="${YELLOW}DISABLED${NC}"
    [ "$NO_RESTART" -eq 1 ] && rest_str="${YELLOW}SKIPPED${NC}" || rest_str="${GREEN}ENABLED${NC}"

    echo ""; echo "=================================================="
    printf "  ${CYAN}CONFIGURATION${NC}\n"
    echo "=================================================="
    if [ -n "$LIST_FILE" ]; then printf "  List file    : %s\n" "$LIST_FILE"
    else printf "  List file    : ${YELLOW}none${NC}\n"; fi
    if [ -n "$EXISTING_FILE" ]; then printf "  Existing file: %s\n" "$EXISTING_FILE"
    else printf "  Existing file: ${YELLOW}none${NC}\n"; fi
    printf "  Output file  : %s\n" "$OUTPUT_FILE"
    printf "  Work dir     : %s\n" "$WORKDIR"
    printf "  Using RAM    : %s\n" "$SORT_RAM"
    printf "  Using threads: %s\n" "$SORT_PARALLEL"
    printf "  Safety check : %b\n" "$safe_str"
    printf "  Dry run      : %b\n" "$dry_str"
    printf "  Auto restart : %b\n" "$rest_str"
    printf "  Pause        : %s sec\n" "$PAUSE"
    echo "=================================================="; echo ""
}

setup_environment() {
    local out_dir
    LOCK_FILE="/tmp/nfqws_updater.lock"
    out_dir=$(dirname "$OUTPUT_FILE")
    mkdir -p "$CACHE_DIR" "$TMP_BASE" "$out_dir"
    TMPDIR="$(mktemp -d "$TMP_BASE/ipset.XXXXXX")"
    touch "$TMPDIR/active_cache.list"
}

acquire_lock() {
    if ! lock -n "$LOCK_FILE"; then
        log_warn "Скрипт уже выполняется. Выходим."
        exit 0
    fi
}

cleanup() {
    lock -u "$LOCK_FILE" 2>/dev/null || true

    if [ -n "${TMPDIR:-}" ] && [ -d "${TMPDIR}" ]; then
        if [[ "$TMPDIR" == "$TMP_BASE/"* ]]; then
            rm -rf "$TMPDIR"
        else
            log_err "Некорректный TMPDIR ($TMPDIR), очистка отменена!"
        fi
    fi
}

safe_load_stats() {   
    local stats_file="$1"
    [ -f "$stats_file" ] || return 0
    while IFS='=' read -r key val; do
        [ -z "$key" ] || [ "${key#\#}" != "$key" ] && continue
        val=$(echo "$val" | sed "s/'//g")
        case "$key" in
            L_V4) L_V4="${val//[^0-9]/}" ;;
            L_V6) L_V6="${val//[^0-9]/}" ;;
            L_TOT) L_TOT="${val//[^0-9]/}" ;;
            L_BYTES) L_BYTES="${val//[^0-9]/}" ;;
            L_TS) L_TS="$val" ;; # Сохраняем строку с датой как есть, без tr
        esac
    done < "$stats_file" 2>/dev/null
}

check_disk_space() {
    local min_kb=$((MAX_FILESIZE_BYTES * 3 / 1024))
    local targets="$WORKDIR $(dirname "$OUTPUT_FILE")"
    local target; local avail_kb; local fs
    for target in $targets; do
        mkdir -p "$target" 2>/dev/null || true
        avail_kb=$(df -k "$target" 2>/dev/null | awk 'NR==2 {print $4}')
        fs=$(df -h "$target" 2>/dev/null | awk 'NR==2 {print $1}')
        if [ -z "$avail_kb" ] || [ "$avail_kb" -lt "$min_kb" ]; then
            log_err "Not enough free space on $fs (target: $target)"
            exit 1
        fi
    done
}

trap 'log_warn "Interrupted! Cleaning up..."; cleanup; exit 130' INT TERM
trap cleanup EXIT

# ==============================================================================
# 4. HELPER FUNCTIONS
# ==============================================================================

check_dependencies() {
    for cmd in curl sipcalc md5sum awk sort timeout; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            log_err "Missing required dependency: $cmd"
            exit 1
        fi
    done
}

send_mqtt_notification() {
    [ "$MQTT_SEND" -ne 1 ] && return 0
    if ! command -v mosquitto_pub >/dev/null 2>&1; then
        log_warn "mosquitto_pub not found, skipping MQTT notification."
        return 0
    fi

    # Используем сгенерированные переменные (напр. nfqws_youtube)
    local node_id="nfqws_${MQTT_SAFE_ID}"
    local dev_json='{"identifiers":["'$node_id'"],"name":"'"$MQTT_DEVICE_NAME"'","manufacturer":"Bash Script","model":"Router DPI Bypass"}'
    
    log_info "Sending MQTT configs to Home Assistant..."
    
    # Время последнего обновления списков
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" -r \
        -t "${MQTT_PREFIX}_status/config" \
        -m '{"name":"Last update", "unique_id":"'${node_id}'_status", "state_topic":"'${MQTT_TOPIC_STATE}'", "value_template":"{{ value_json.last_update }}", "json_attributes_topic":"'${MQTT_TOPIC_STATE}'", "device_class":"timestamp", "icon":"mdi:clock-check-outline", "device":'"$dev_json"'}' &

    # Время последнего запуска скрипта
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" -r \
        -t "${MQTT_PREFIX}_last_run/config" \
        -m '{"name":"Last run", "unique_id":"'${node_id}'_last_run", "state_topic":"'${MQTT_TOPIC_STATE}'", "value_template":"{{ value_json.start_time }}", "device_class":"timestamp", "icon":"mdi:script-text-play", "device":'"$dev_json"'}' &

    # Всего IP
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" -r \
        -t "${MQTT_PREFIX}_ips/config" \
        -m '{"name":"Total IP ranges", "unique_id":"'${node_id}'_ips", "state_topic":"'${MQTT_TOPIC_STATE}'", "value_template":"{{ value_json.total_ips }}", "unit_of_measurement":"IPs", "icon":"mdi:ip-network", "state_class":"measurement", "device":'"$dev_json"'}' &

    # IPv4
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" -r \
        -t "${MQTT_PREFIX}_ipv4/config" \
        -m '{"name":"IPv4 ranges", "unique_id":"'${node_id}'_ipv4", "state_topic":"'${MQTT_TOPIC_STATE}'", "value_template":"{{ value_json.ipv4 }}", "unit_of_measurement":"IPs", "icon":"mdi:numeric-4-box-multiple", "state_class":"measurement", "device":'"$dev_json"'}' &

    # IPv6
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" -r \
        -t "${MQTT_PREFIX}_ipv6/config" \
        -m '{"name":"IPv6 ranges", "unique_id":"'${node_id}'_ipv6", "state_topic":"'${MQTT_TOPIC_STATE}'", "value_template":"{{ value_json.ipv6 }}", "unit_of_measurement":"IPs", "icon":"mdi:numeric-6-box-multiple", "state_class":"measurement", "device":'"$dev_json"'}' &

    # Размер файла
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" -r \
        -t "${MQTT_PREFIX}_size/config" \
        -m '{"name":"File size", "unique_id":"'${node_id}'_size", "state_topic":"'${MQTT_TOPIC_STATE}'", "value_template":"{{ value_json.size_bytes }}", "unit_of_measurement":"B", "device_class":"data_size", "state_class":"measurement", "device":'"$dev_json"'}' &

    # Отправка payload (основные данные)
    local payload
    payload=$(printf '{"start_time": "%s", "last_update": "%s", "total_ips": %s, "ipv4": %s, "ipv6": %s, "size_bytes": %s}' \
              "$START_TS" "$SHOW_TS" "$STAT_TOTAL" "$STAT_V4" "$STAT_V6" "$STAT_BYTES")
              
    mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -u "$MQTT_USER" -P "$MQTT_PASS" -r \
        -t "$MQTT_TOPIC_STATE" -m "$payload" &
        
    # Ждем, пока все фоновые процессы отправки завершатся
    wait
    
    log_succ "MQTT payload and configs sent successfully!"
}

extract_domain() { echo "$1" | awk -F/ '{print $3}'; }

fetch_source() {
    local type="$1"; local src="$2"; local domain_label="$3"
    local id
    id=$(printf "%s" "$src" | md5sum | cut -d' ' -f1)
    local fname="${id}.txt"; local cache_path="$CACHE_DIR/$fname"
    local work_path="$TMPDIR/$fname"; local tmp_dl="$TMPDIR/$fname.dl"
    
    echo "$fname" >> "${TMPDIR:-}/active_cache_${BASHPID}_${RANDOM}.list"

    local short_name=""
    if [ "$type" = "file" ]; then
        short_name="Local"
    else
        short_name=$(echo "$id" | cut -c1-8)
    fi
    local status="UNKNOWN"; local color="$RED"
    
    if [ "$type" = "file" ]; then
        if [ ! -f "$src" ]; then status="MISSING"; color="$RED"
        else
            local needs_cp=0
            if [ ! -f "$cache_path" ]; then needs_cp=1
            else
                local s_sum c_sum
                s_sum=$(md5sum "$src" | cut -d' ' -f1)
                c_sum=$(md5sum "$cache_path" | cut -d' ' -f1)
                [ "$s_sum" != "$c_sum" ] && needs_cp=1
            fi
            if [ "$needs_cp" -eq 1 ]; then cp "$src" "$cache_path"; status="UPDATED"; color="$GREEN"
            else status="CACHE"; color="$YELLOW"; fi
        fi
    else 
        if [ -f "$cache_path" ]; then
            if curl $CURL_OPTS -z "$cache_path" -o "$tmp_dl" "$src" 2>/dev/null; then
                if [ -s "$tmp_dl" ]; then mv "$tmp_dl" "$cache_path"; status="UPDATED"; color="$GREEN"
                else status="CACHE"; color="$YELLOW"; rm -f "$tmp_dl"; fi
            else status="FAIL(Cache)"; color="$RED"; rm -f "$tmp_dl"; fi
        else
            if curl $CURL_OPTS -o "$tmp_dl" "$src" 2>/dev/null; then
                if [ -s "$tmp_dl" ]; then mv "$tmp_dl" "$cache_path"; status="NEW"; color="$GREEN"
                else status="EMPTY"; color="$RED"; rm -f "$tmp_dl"; fi
            else status="ERROR"; color="$RED"; rm -f "$tmp_dl"; fi
        fi
    fi
    if [ -f "$cache_path" ] && [ "$status" != "MISSING" ]; then cp "$cache_path" "$work_path"; fi
    
    # Синхронный вывод через OpenWrt lock
    lock "$TMP_BASE/print.lock"
    printf " [%-32s] %-10s -> ${color}%s${NC}\n" "$domain_label" "$short_name" "$status"
    lock -u "$TMP_BASE/print.lock"
}

cleanup_old_cache() {
    log_step "Cleaning up obsolete cache files..."
    local count=0
    
    for f in "$CACHE_DIR"/*.txt; do
        [ -e "$f" ] || continue
        local bname
        bname=$(basename "$f")
        if ! grep -qF "$bname" "${TMPDIR:-}/active_cache.list" 2>/dev/null; then
             rm -f "$f"
             count=$((count+1))
        fi
    done
    
    if [ "$count" -gt 0 ]; then
        log_succ "Removed $count unused files."
    else
        log_info "Cache is clean."
    fi
}

validate_cidr() {
    local type=$1; local infile="${TMPDIR:-}/$type.raw"; local outfile="${TMPDIR:-}/$type.valid"
    [ ! -f "$infile" ] && touch "$outfile" && return
    local lines
    lines=$(wc -l < "$infile" 2>/dev/null || echo "???")
    printf " Validating ${CYAN}%-4s${NC} ranges (%s lines)... " "$type" "$lines"
    > "$outfile"
    
    if [ "$type" = "ipv6" ]; then
        while IFS= read -r line || [ -n "$line" ]; do
            local expanded
            expanded=$(timeout 0.5 sipcalc "$line" 2>/dev/null | awk '/Expanded Address/ {print $NF}' || true)
            if [ -n "$expanded" ]; then printf "%s\t%s\n" "$expanded" "$line" >> "$outfile"; fi
        done < "$infile"
    else
        while IFS= read -r line || [ -n "$line" ]; do 
            sipcalc -c "$line" >/dev/null 2>&1 && echo "$line" >> "$outfile"
        done < "$infile"
    fi
    local valid
    valid=$(wc -l < "$outfile" 2>/dev/null || echo "0")
    printf "${GREEN}Done${NC} (Valid: $valid)\n"
}

sort_list() {
    local type="$1"; shift
    local infile="${TMPDIR:-}/$type.valid"; local outfile="${TMPDIR:-}/$type.sorted"
    printf " Sorting ${CYAN}%-4s${NC} " "$type"
    if [ ! -f "$infile" ]; then touch "$outfile"; echo "(Skipped)"; return; fi
    
    if [ "$type" = "ipv4" ] && command -v iprange >/dev/null 2>&1; then
        printf "(Optimizing with iprange)... "
        if iprange "$infile" > "$outfile" 2>/dev/null; then
            printf "${GREEN}Done${NC}\n"
            return 0
        else
            printf "${YELLOW}iprange failed, falling back to awk...${NC}\n"
        fi
    fi
    
    local cmd_sort="sort"
    if sort --parallel=1 -S 1M < /dev/null >/dev/null 2>&1; then
        cmd_sort="sort --parallel=$SORT_PARALLEL -S $SORT_RAM"
    fi
    
    local awk_ipv4_prep='
    function ip2int(ip) { split(ip, a, "."); return a[1]*16777216 + a[2]*65536 + a[3]*256 + a[4]; }
    {
        split($0, p, "/"); ip=p[1]; mask=(p[2]==""?32:p[2]);
        if(mask<0 || mask>32) next;
        ival=ip2int(ip);
        hostmask = (2 ^ (32-mask)) - 1;
        start_int = ival - (ival % (hostmask + 1)); end_int = start_int + hostmask;
        print start_int "\t" end_int "\t" $0
    }
    '
    if [ "$type" = "ipv6" ]; then
        printf "(Sorting)... "
        if $cmd_sort -u -k1,1 "$infile" 2>/dev/null | cut -f2 > "$outfile"; then printf "${GREEN}Done${NC}\n"
        else printf "${RED}Failed${NC}\n"; return 1; fi
    else
        printf "(Sorting)... "
        if awk "$awk_ipv4_prep" "$infile" | $cmd_sort -k1,1n -k2,2rn | awk "$AWK_SCRIPT_OPTIMIZER" > "$outfile"; then 
            printf "${GREEN}Done${NC}\n"
        else printf "${RED}Failed${NC}\n"; return 1; fi
    fi
}

calc_stats() {
    STAT_V4=$(wc -l < "${TMPDIR:-}/ipv4.sorted" 2>/dev/null | tr -d '[:space:]' || echo 0)
    STAT_V6=$(wc -l < "${TMPDIR:-}/ipv6.sorted" 2>/dev/null | tr -d '[:space:]' || echo 0)
    STAT_TOTAL=$(wc -l < "$TMP_OUTPUT_FILE" 2>/dev/null | tr -d '[:space:]' || echo 0)
    STAT_BYTES=$(wc -c < "$TMP_OUTPUT_FILE" 2>/dev/null | tr -d '[:space:]' || echo 0)
}

guard_rails() {
    if [ "$SKIP_SAFETY" -eq 1 ]; then log_warn "Safety check DISABLED"; return 0; fi

    # 1. Жесткая проверка минимального количества строк (ВСЕГДА)
    if [ "$STAT_TOTAL" -lt "$MIN_LINES" ]; then 
        log_err "SAFETY TRIGGER: Too small (< $MIN_LINES lines)."
        rm -f "$TMP_OUTPUT_FILE"; exit 1
    fi

    # 2. Проверка аномального падения объема относительно старой версии
    if [ -f "$OUTPUT_FILE" ]; then
        local old_total limit=0
        old_total=$(wc -l < "$OUTPUT_FILE" 2>/dev/null | tr -d '[:space:]' || echo 0)
        [ -z "$old_total" ] && old_total=0
        
        if [ "$old_total" -gt 0 ]; then limit=$(( (old_total * THRESHOLD_PERCENT) / 100 )); fi
        log_info "Safety check: Old=$old_total, New=$STAT_TOTAL, Limit=$limit"
        
        if [ "$STAT_TOTAL" -lt "$limit" ]; then 
            log_err "SAFETY TRIGGER: New list size is too small (-${THRESHOLD_PERCENT}% drop)."
            log_warn "If intended, run with -S to skip safety check."
            rm -f "$TMP_OUTPUT_FILE"; exit 1
        fi
    fi
}

compare_and_report() {
    local stats_file="$CACHE_DIR/.stats"; local head_color="${GREEN}"
    CHANGED=1; L_V4="0"; L_V6="0"; L_TOT="0"; L_BYTES="0"; L_TS=""
    safe_load_stats "$stats_file"
    local current_ts
    current_ts=$(date "+%Y-%m-%dT%H:%M:%S%z")
    
    if [ -z "$L_TS" ] && [ -f "$OUTPUT_FILE" ]; then 
        L_TS=$(date -r "$OUTPUT_FILE" "+%Y-%m-%dT%H:%M:%S%z" 2>/dev/null || echo "$current_ts")
    fi

    if [ -f "$OUTPUT_FILE" ]; then
        local sum_new sum_old
        sum_new=$(md5sum "$TMP_OUTPUT_FILE" | cut -d' ' -f1)
        sum_old=$(md5sum "$OUTPUT_FILE" | cut -d' ' -f1)
        if [ "$sum_new" = "$sum_old" ]; then CHANGED=0; head_color="${YELLOW}"; fi
    fi
    if [ "$CHANGED" -eq 1 ]; then SHOW_TS="$current_ts"; else SHOW_TS="${L_TS:-$current_ts}"; fi
    echo ""; echo "=================================================="
    printf "  ${head_color}STATISTICS${NC}\n"
    echo "=================================================="
    print_row() { awk -v lbl="$1" -v curr="$2" -v last="$3" -v is_bytes="$4" -v red="$RED" -v green="$GREEN" -v nc="$NC" "$AWK_SCRIPT_STATS"; }
    print_row "IPv4 ranges" "$STAT_V4" "$L_V4" 0
    print_row "IPv6 ranges" "$STAT_V6" "$L_V6" 0
    print_row "Total ranges" "$STAT_TOTAL" "$L_TOT" 0
    print_row "File size" "$STAT_BYTES" "$L_BYTES" 1
    printf "  %-13s: %s\n" "Last updated" "$SHOW_TS"
    echo "=================================================="; echo ""
}

apply_changes() {
    if [ "$DRY_RUN" -eq 1 ]; then log_warn "DRY RUN: No changes applied."; return 0; fi
    local stats_file="$CACHE_DIR/.stats"
    if mv "$TMP_OUTPUT_FILE" "$OUTPUT_FILE"; then
        echo "[$(date)] OK: total=$STAT_TOTAL v4=$STAT_V4 v6=$STAT_V6 size=$STAT_BYTES output=$OUTPUT_FILE" >> "$LOG_FILE"
        logger -t nfqws-updater "Успешное обновление списков: $STAT_TOTAL IP-диапазонов (IPv4: $STAT_V4, IPv6: $STAT_V6)" 2>/dev/null || true
        echo "L_V4=$STAT_V4" > "$stats_file"; echo "L_V6=$STAT_V6" >> "$stats_file"
        echo "L_TOT=$STAT_TOTAL" >> "$stats_file"; echo "L_BYTES=$STAT_BYTES" >> "$stats_file"
        echo "L_TS='$SHOW_TS'" >> "$stats_file"
        
        if [ "$NO_RESTART" -eq 0 ]; then
            if command -v service >/dev/null 2>&1; then 
                echo "Restarting $SERVICE_NAME..."
                service "$SERVICE_NAME" restart || echo "Warning: Failed to restart service" >&2
            else log_warn "Service command not found, restart skipped."; fi
        else 
            log_warn "Service restart SKIPPED (flag -R active)."
        fi
               
    else log_err "Failed to move output file!"; exit 1; fi
}

phase_download() {
    if [ -n "$EXISTING_FILE" ] && [ -f "$EXISTING_FILE" ]; then
        log_step "Processing local file..."
        fetch_source "file" "$EXISTING_FILE" "Local file"
    fi
    local count_urls
    count_urls=$(echo "$URLS_CONTENT" | grep -c . || echo 0)
    if [ "$count_urls" -gt 0 ]; then
        local queue_dir="${TMPDIR:-}/queues"
        mkdir -p "$queue_dir"
        log_step "Grouping $count_urls URLs by domain..."
        echo "$URLS_CONTENT" | while IFS= read -r url; do
            [ -z "$url" ] && continue
            local domain
            domain=$(extract_domain "$url")
            [ -z "$domain" ] && domain="unknown"
            echo "$url" >> "$queue_dir/${domain}.list"
        done
        local domain_count
        domain_count=$(ls "$queue_dir"/*.list 2>/dev/null | wc -l)
        log_step "Starting parallel downloads in background (Max: $SORT_PARALLEL)..."
        echo ""
        log_info "Processing..."
        echo ""
        for queue_file in "$queue_dir"/*.list; do
            [ -e "$queue_file" ] || continue
            
            while [ "$(jobs -p | wc -l)" -ge "$SORT_PARALLEL" ]; do
                sleep 1
            done
            (
                set +e 
                local dom_name
                dom_name=$(basename "$queue_file" .list)
                local idx=0
                while IFS= read -r target_url || [ -n "$target_url" ]; do
                    [ -z "$target_url" ] && continue
                    idx=$((idx+1))
                    [ "$idx" -gt 1 ] && sleep "$PAUSE"
                    fetch_source "url" "$target_url" "$dom_name"
                done < "$queue_file"
            ) & 
        done
        
        wait
        echo ""
        log_succ "All downloads finished."
    else
        log_info "No URLs to download."
    fi
    
    if ls "${TMPDIR:-}"/active_cache_*.list >/dev/null 2>&1; then
        cat "${TMPDIR:-}"/active_cache_*.list > "${TMPDIR:-}/active_cache.list"
        cleanup_old_cache
    else
        log_warn "No cache lists generated. Skipping cache cleanup to avoid data loss."
    fi
}

# Оптимизация сборки текстовых файлов перед awk
phase_process() {
    log_step "Processing and merging lists..."
    find "${TMPDIR:-}" -name "*.txt" -type f -print0 2>/dev/null | \
        xargs -0 -r cat | \
        awk -v outdir="${TMPDIR:-}" "$AWK_SCRIPT_CLEANER"
}

phase_validate() { log_step "Validating..."; validate_cidr "ipv4"; validate_cidr "ipv6"; }

phase_sort() { log_step "Optimizing..."; sort_list "ipv4"; sort_list "ipv6"; }

phase_finalize() {
    log_step "Creating output..."
    cat "${TMPDIR:-}/ipv4.sorted" "${TMPDIR:-}/ipv6.sorted" > "$TMP_OUTPUT_FILE"
    calc_stats
    guard_rails
    compare_and_report
    local needs_update=0
    if [ "$CHANGED" -eq 1 ]; then needs_update=1; elif [ "$L_TOT" -eq 0 ]; then
        echo "L_TS='$SHOW_TS'" >> "$CACHE_DIR/.stats"
    fi
    if [ "$needs_update" -eq 1 ]; then apply_changes; else rm -f "$TMP_OUTPUT_FILE"; fi
}

main() {
    init_configuration "$@"
    check_dependencies
    setup_environment
    acquire_lock
    check_disk_space
    print_config_table
    phase_download
    phase_process
    phase_validate
    phase_sort
    phase_finalize
    send_mqtt_notification
}

main "$@"
