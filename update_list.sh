#!/bin/sh

export PATH=/usr/sbin:/usr/bin:/sbin:/bin
set -euo pipefail

# ==============================================================================
# 1. CONSTANTS & DEFAULTS
# ==============================================================================

if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    NC='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BLUE=''; CYAN=''; NC=''
fi

log_info() { printf "${BLUE}INFO:${NC} %s\n" "$1"; }
log_step() { printf "${CYAN}==>${NC} %s\n" "$1"; }
log_succ() { printf "${GREEN}OK:${NC} %s\n" "$1"; }
log_warn() { printf "${YELLOW}WARN:${NC} %s\n" "$1"; }
log_err()  { printf "${RED}ERR:${NC} %s\n" "$1"; }

DEFAULT_WORKDIR="/tmp/nfqws_updater"
DEFAULT_PAUSE=2
SERVICE_NAME="nfqws-keenetic"
MIN_LINES=10
THRESHOLD_PERCENT=90
DRY_RUN=0
MAX_FILESIZE_BYTES=10485760  # 10 MB
LOG_FILE="/tmp/firewall_update.log"
SKIP_SAFETY=0
NO_RESTART=0

# ==============================================================================
# 2. EMBEDDED AWK SCRIPTS
# ==============================================================================

AWK_SCRIPT_CLEANER='
    { sub(/#.*/, "") }
    { gsub(/["\[\]{},]/, "") }
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
    # Smart RAM detection
    SORT_RAM="$(awk '/MemAvailable/ {
        kb = $2;
        mb = int((kb / 1024) * 0.5);  # 50% of available RAM
        if (mb < 64) mb = 64;     # Min floor: 64MB
        if (mb > 4096) mb = 4096; # Max cap: 4GB
        printf "%dM", mb
    }' /proc/meminfo 2>/dev/null || echo '64M')"
    
    # Safe CPU detection
    local cpu_count
    cpu_count=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 1)
    SORT_PARALLEL="$((cpu_count - 1))"
    [ "$SORT_PARALLEL" -lt 1 ] && SORT_PARALLEL=1

    local workdir_arg=""; local pause_arg=""
    
    while getopts "f:o:e:w:t:SRd" opt; do
        case $opt in
            f) LIST_FILE="$OPTARG" ;;
            o) OUTPUT_FILE="$OPTARG" ;;
            e) EXISTING_FILE="$OPTARG" ;;
            w) workdir_arg="$OPTARG" ;;
            t) pause_arg="$OPTARG" ;;
            S) SKIP_SAFETY=1 ;;
            R) NO_RESTART=1 ;;
            d) DRY_RUN=1 ;;
            *) log_warn "Unknown option -$OPTARG";;
        esac
    done
    if [ -n "$pause_arg" ] && echo "$pause_arg" | grep -qE '^[0-9]+$'; then PAUSE="$pause_arg"; else PAUSE="$DEFAULT_PAUSE"; fi
    
    CURL_OPTS="-sSLfR --connect-timeout 15 --max-time 60 --max-filesize $MAX_FILESIZE_BYTES --retry 3 --retry-delay $PAUSE"
    WORKDIR="${workdir_arg:-$DEFAULT_WORKDIR}"; WORKDIR="${WORKDIR%/}"
    CACHE_DIR="${WORKDIR}/cache"; TMP_BASE="${WORKDIR}/temp"
    if [ -z "$OUTPUT_FILE" ]; then
        local script_dir="$(cd "$(dirname "$0")" && pwd)"
        OUTPUT_FILE="${script_dir}/ipset_include.txt"
    fi
    TMP_OUTPUT_FILE="${OUTPUT_FILE}.tmp"
    if [ -n "$LIST_FILE" ] && [ -f "$LIST_FILE" ]; then
        URLS_CONTENT=$(grep -vE '^\s*#|^\s*$' "$LIST_FILE" | tr -d '\r' || true)
    fi

    # Resource-based lock identifier (after paths are resolved)
    LOCK_ID=$(printf "%s:%s" "$WORKDIR" "$OUTPUT_FILE" | md5sum | cut -c1-12)
    LOCK_FILE="/tmp/lock/nfqws_update.${LOCK_ID}.lock"
}

print_config_table() {
    echo ""; echo "=================================================="
    printf "  ${CYAN}CONFIGURATION${NC}\n"
    echo "=================================================="
    printf "  List File    : %s\n" "${LIST_FILE:-(none)}"
    printf "  Existing File: %s\n" "${EXISTING_FILE:-(none)}"
    printf "  Output File  : %s\n" "$OUTPUT_FILE"
    printf "  Work Dir     : %s\n" "$WORKDIR"
    printf "  Sort RAM     : %s\n" "$SORT_RAM"
    printf "  Sort Threads : %s\n" "$SORT_PARALLEL"
    printf "  Service      : %s\n" "$SERVICE_NAME"
    printf "  Safety Check : %s\n" "$([ "$SKIP_SAFETY" -eq 1 ] && printf "${RED}DISABLED${NC}" || printf "${GREEN}ENABLED${NC}")"
    printf "  Auto Restart : %s\n" "$([ "$NO_RESTART" -eq 1 ] && printf "${RED}DISABLED${NC}" || printf "${GREEN}ENABLED${NC}")"
    printf "  Dry Run      : %s\n" "$([ "$DRY_RUN" -eq 1 ] && printf "${GREEN}ENABLED${NC}" || printf "${RED}DISABLED${NC}")"
    echo "=================================================="; echo ""
}

setup_environment() {
    mkdir -p "$CACHE_DIR" "$TMP_BASE" "$(dirname "$LOCK_FILE")" "$(dirname "$OUTPUT_FILE")"
    TMPDIR="$(mktemp -d "$TMP_BASE/ipset.XXXXXX")"
}

acquire_lock() {
    # Open lock file descriptor (kept open for entire script lifetime)
    exec 200>"$LOCK_FILE"
    # Attempt non-blocking lock acquisition
    if ! flock -n 200; then
        local other_pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "?")
        log_warn "Resource locked (WORKDIR='$WORKDIR', OUTPUT='$OUTPUT_FILE'). Another instance running (PID: $other_pid). Exiting."
        exit 0
    fi
    # Store our PID in the lock file for diagnostics
    echo $$ >&200
    # Descriptor 200 remains open > lock held until script exits (even on crash)
}

cleanup() {
    rm -rf "${TMPDIR:-}"
}

# Safe parser to prevent code injection (CVE mitigation)
safe_load_stats() {   
    local stats_file="$1"
    [ -f "$stats_file" ] || return 0
    
    while IFS='=' read -r key val; do
        # Skip empty lines and comments
        [ -z "$key" ] || [ "${key#\#}" != "$key" ] && continue
        
        case "$key" in
            L_V4) L_V4="${val//[^0-9]/}" ;;
            L_V6) L_V6="${val//[^0-9]/}" ;;
            L_TOT) L_TOT="${val//[^0-9]/}" ;;
            L_BYTES) L_BYTES="${val//[^0-9]/}" ;;
            L_TS) 
                # Allow only digits, hyphens, colons, spaces for timestamp
                L_TS="$(printf '%s' "$val" | tr -cd '0-9:- ' | head -c 30)"
                ;;
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
            log_err "Required: ${min_kb} KB, Available: ${avail_kb:-?} KB"
            log_err "Free up space or use -w to specify alternative work directory."
            exit 1
        fi
    done
    
    log_info "Disk space OK (min ${min_kb} KB required)"
}

trap 'log_warn "Interrupted! Cleaning up..."; cleanup; exit 130' INT TERM
trap cleanup EXIT

# ==============================================================================
# 4. HELPER FUNCTIONS
# ==============================================================================

fetch_source() {
    local type="$1"; local src="$2"; local idx="$3"; local total="$4"
    local id=$(printf "%s" "$src" | md5sum | cut -d' ' -f1)
    local fname="${id}.txt"; local cache_path="$CACHE_DIR/$fname"
    local work_path="$TMPDIR/$fname"; local tmp_dl="$TMPDIR/$fname.dl"
    
    local label=""
    if [ "$type" = "file" ]; then label="Local file $(basename "$src" | cut -c1-45)..."
    else label="List ID $(echo "$id" | cut -c1-8)..."; fi
    printf " [%d/%d] %-50s " "$idx" "$total" "$label"
    local status="UNKNOWN"; local color="$RED"
    
    if [ "$type" = "file" ]; then
        if [ ! -f "$src" ]; then printf "${RED}[MISSING]${NC}\n"; return; fi
        local needs_cp=0
        if [ ! -f "$cache_path" ]; then needs_cp=1
        else
            local s_sum=$(md5sum "$src" | cut -d' ' -f1)
            local c_sum=$(md5sum "$cache_path" | cut -d' ' -f1)
            [ "$s_sum" != "$c_sum" ] && needs_cp=1
        fi
        if [ "$needs_cp" -eq 1 ]; then cp "$src" "$cache_path"; status="UPDATED"; color="$GREEN"
        else status="CACHE"; color="$YELLOW"; fi
    else 
        if [ -f "$cache_path" ]; then
            if curl $CURL_OPTS -z "$cache_path" -o "$tmp_dl" "$src" 2>/dev/null; then
                if [ -s "$tmp_dl" ]; then mv "$tmp_dl" "$cache_path"; status="UPDATED"; color="$GREEN"
                else status="CACHE"; color="$YELLOW"; rm -f "$tmp_dl"; fi
            else status="FAIL (Cache)"; color="$RED"; rm -f "$tmp_dl"; fi
        else
            if curl $CURL_OPTS -o "$tmp_dl" "$src" 2>/dev/null; then
                if [ -s "$tmp_dl" ]; then mv "$tmp_dl" "$cache_path"; status="NEW"; color="$GREEN"
                else status="EMPTY"; color="$RED"; rm -f "$tmp_dl"; fi
            else status="ERROR"; color="$RED"; rm -f "$tmp_dl"; fi
        fi
    fi
    printf "${color}[%s]${NC}\n" "$status"
    if [ -f "$cache_path" ]; then cp "$cache_path" "$work_path"; fi
}

validate_cidr() {
    local type=$1; local infile="$TMPDIR/$type.raw"; local outfile="$TMPDIR/$type.valid"
    [ ! -f "$infile" ] && touch "$outfile" && return
    local lines=$(wc -l < "$infile" 2>/dev/null || echo "???")
    printf " Validating ${CYAN}%-4s${NC} ranges (%s lines)... " "$type" "$lines"
    > "$outfile"
    
    if [ "$type" = "ipv6" ]; then
        while IFS= read -r line || [ -n "$line" ]; do
            local expanded=$(timeout 0.5 sipcalc "$line" 2>/dev/null | awk '/Expanded Address/ {print $NF}' || true)
            if [ -n "$expanded" ]; then
                printf "%s\t%s\n" "$expanded" "$line" >> "$outfile"
            fi
        done < "$infile"
    else
        while IFS= read -r line || [ -n "$line" ]; do 
            sipcalc -c "$line" >/dev/null 2>&1 && echo "$line" >> "$outfile"
        done < "$infile"
    fi
    local valid=$(wc -l < "$outfile" 2>/dev/null || echo "0")
    printf "${GREEN}Done${NC} (Valid: $valid)\n"
}

sort_list() {
    local type="$1"; shift
    local infile="$TMPDIR/$type.valid"; local outfile="$TMPDIR/$type.sorted"
    printf " Sorting ${CYAN}%-4s${NC} " "$type"
    if [ ! -f "$infile" ]; then touch "$outfile"; echo "(Skipped)"; return; fi
    local cmd_sort="sort --parallel=$SORT_PARALLEL -S $SORT_RAM"
    
    local awk_ipv4_prep='
    function ip2int(ip) {
        split(ip, a, ".");
        return a[1]*16777216 + a[2]*65536 + a[3]*256 + a[4];
    }
    {
        split($0, p, "/");
        ip=p[1]; mask=(p[2]==""?32:p[2]);
        if(mask<0 || mask>32) next;
        ival=ip2int(ip);
        hostmask = (2 ^ (32-mask)) - 1;
        start_int = ival - (ival % (hostmask + 1))
        end_int = start_int + hostmask
        print start_int "\t" end_int "\t" $0
    }
    '
    if [ "$type" = "ipv6" ]; then
        printf "(Canonical Sort)... "
        if $cmd_sort -u -k1,1 "$infile" 2>/dev/null | cut -f2 > "$outfile"; then
            printf "${GREEN}Done${NC}\n"
        else
            printf "${RED}Failed${NC}\n"; return 1
        fi
    else
        printf "(Calc & Overlay Check)... "
        if awk "$awk_ipv4_prep" "$infile" | \
           $cmd_sort -k1,1n -k2,2rn | \
           awk "$AWK_SCRIPT_OPTIMIZER" > "$outfile"; then 
            printf "${GREEN}Done${NC}\n"
        else 
            printf "${RED}Failed${NC}\n"; return 1
        fi
    fi
}

calc_stats() {
    STAT_V4=$(wc -l < "$TMPDIR/ipv4.sorted" 2>/dev/null || echo 0)
    STAT_V4=$(echo "$STAT_V4" | tr -d '[:space:]')
    
    STAT_V6=$(wc -l < "$TMPDIR/ipv6.sorted" 2>/dev/null || echo 0)
    STAT_V6=$(echo "$STAT_V6" | tr -d '[:space:]')
    
    STAT_TOTAL=$(wc -l < "$TMP_OUTPUT_FILE" 2>/dev/null || echo 0)
    STAT_TOTAL=$(echo "$STAT_TOTAL" | tr -d '[:space:]')
    
    STAT_BYTES=$(wc -c < "$TMP_OUTPUT_FILE" 2>/dev/null || echo 0)
    STAT_BYTES=$(echo "$STAT_BYTES" | tr -d '[:space:]')
}

guard_rails() {
    if [ "$SKIP_SAFETY" -eq 1 ]; then
        log_warn "Safety check DISABLED"
        return 0
    fi
    if [ -f "$OUTPUT_FILE" ]; then
        local old_total=$(wc -l < "$OUTPUT_FILE" 2>/dev/null || echo 0)
        old_total=$(echo "$old_total" | tr -d '[:space:]') # Sanitize
        
        if [ -z "$old_total" ]; then old_total=0; fi
        
        local limit=0
        if [ "$old_total" -gt 0 ]; then
             limit=$(( (old_total * THRESHOLD_PERCENT) / 100 ))
        fi
        log_info "Safety check: Old=$old_total, New=$STAT_TOTAL, Limit=$limit"
        
        if [ "$STAT_TOTAL" -lt "$limit" ]; then 
            log_err "SAFETY TRIGGER: New list size is too small (-${THRESHOLD_PERCENT}% drop)."
            log_warn "If this is due to list changes or new optimization, run with -S to skip safety check."
            rm -f "$TMP_OUTPUT_FILE"
            exit 1
        fi
    elif [ "$STAT_TOTAL" -lt "$MIN_LINES" ]; then 
        log_err "SAFETY TRIGGER: Too small (< $MIN_LINES lines)."
        rm -f "$TMP_OUTPUT_FILE"
        exit 1
    fi
}

compare_and_report() {
    local stats_file="$CACHE_DIR/.stats"
    local head_color="${GREEN}"
    CHANGED=1
    L_V4="0"; L_V6="0"; L_TOT="0"; L_BYTES="0"; L_TS=""
    
    safe_load_stats "$stats_file"
    local current_ts=$(date "+%Y-%m-%d %H:%M:%S")
    if [ -z "$L_TS" ] && [ -f "$OUTPUT_FILE" ]; then 
        L_TS=$(date -r "$OUTPUT_FILE" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "$current_ts")
    fi
    if [ -f "$OUTPUT_FILE" ]; then
        local sum_new=$(md5sum "$TMP_OUTPUT_FILE" | cut -d' ' -f1)
        local sum_old=$(md5sum "$OUTPUT_FILE" | cut -d' ' -f1)
        if [ "$sum_new" = "$sum_old" ]; then CHANGED=0; head_color="${YELLOW}"; fi
    fi
    if [ "$CHANGED" -eq 1 ]; then SHOW_TS="$current_ts"; else SHOW_TS="${L_TS:-$current_ts}"; fi
    echo ""; echo "=================================================="
    printf "  ${head_color}STATISTICS${NC}\n"
    echo "=================================================="
    print_row() {
        awk -v lbl="$1" -v curr="$2" -v last="$3" -v is_bytes="$4" \
            -v red="$RED" -v green="$GREEN" -v nc="$NC" \
            "$AWK_SCRIPT_STATS"
    }
    print_row "IPv4 ranges" "$STAT_V4" "$L_V4" 0
    print_row "IPv6 ranges" "$STAT_V6" "$L_V6" 0
    print_row "Total ranges" "$STAT_TOTAL" "$L_TOT" 0
    print_row "File size" "$STAT_BYTES" "$L_BYTES" 1
    printf "  %-13s: %s\n" "Last updated" "$SHOW_TS"
    echo "=================================================="; echo ""
}

apply_changes() {
    if [ "$DRY_RUN" -eq 1 ]; then
        log_warn "DRY RUN: Changes NOT applied (output file would be: $OUTPUT_FILE)"
        if [ "$NO_RESTART" -eq 0 ]; then
            log_warn "DRY RUN: Service restart would be triggered for $SERVICE_NAME"
        fi
        return 0
    fi
    local stats_file="$CACHE_DIR/.stats"
    if mv "$TMP_OUTPUT_FILE" "$OUTPUT_FILE"; then
        echo "[$(date)] OK: total=$STAT_TOTAL v4=$STAT_V4 v6=$STAT_V6 size=$STAT_BYTES output=$OUTPUT_FILE" >> "$LOG_FILE"
        echo "L_V4=$STAT_V4" > "$stats_file"; echo "L_V6=$STAT_V6" >> "$stats_file"
        echo "L_TOT=$STAT_TOTAL" >> "$stats_file"; echo "L_BYTES=$STAT_BYTES" >> "$stats_file"
        echo "L_TS='$SHOW_TS'" >> "$stats_file"
        
        # Check restart flag
        if [ "$NO_RESTART" -eq 0 ]; then
            if command -v service >/dev/null 2>&1; then 
                echo "Restarting $SERVICE_NAME..."
                service "$SERVICE_NAME" restart || echo "Warning: Failed to restart service" >&2
            else
                log_warn "Service command not found, restart skipped."
            fi
        else
            log_warn "Service restart SKIPPED (flag -r active)."
        fi
    else log_err "Failed to move output file!"; exit 1; fi
}

phase_download() {
    local count_urls=$(echo "$URLS_CONTENT" | grep -c . || echo 0)
    local count_local=0
    [ -n "$EXISTING_FILE" ] && [ -f "$EXISTING_FILE" ] && count_local=1
    local total_tasks=$((count_urls + count_local))
    if [ "$total_tasks" -gt 0 ]; then
        log_step "Download lists ($total_tasks sources)..."
        echo ""
        local i=0
        if [ "$count_local" -eq 1 ]; then
            i=$((i + 1))
            fetch_source "file" "$EXISTING_FILE" "$i" "$total_tasks"
        elif [ -n "$EXISTING_FILE" ]; then log_warn "Existing file specified but not found: $EXISTING_FILE"; fi
        if [ "$count_urls" -gt 0 ]; then
            echo "$URLS_CONTENT" | while IFS= read -r url; do
                [ -z "$url" ] && continue; i=$((i + 1))
                fetch_source "url" "$url" "$i" "$total_tasks"
                [ "$i" -lt "$total_tasks" ] && sleep $PAUSE; true
            done
        fi
        echo ""
    else log_info "No sources provided."; fi
}

phase_process() {
    log_step "Processing and merging lists..."
    { find "$TMPDIR" -name "*.txt" -type f 2>/dev/null | while read -r f; do cat "$f"; echo ""; done || true; } | \
    awk -v outdir="$TMPDIR" "$AWK_SCRIPT_CLEANER"
}

phase_validate() {
    log_step "Validating..."; validate_cidr "ipv4"; validate_cidr "ipv6"
}

phase_sort() {
    log_step "Optimizing..."; sort_list "ipv4"; sort_list "ipv6"
}

phase_finalize() {
    log_step "Creating output..."
    cat "$TMPDIR/ipv4.sorted" "$TMPDIR/ipv6.sorted" > "$TMP_OUTPUT_FILE"
    calc_stats
    guard_rails
    compare_and_report
    local needs_update=0
    if [ "$CHANGED" -eq 1 ]; then needs_update=1; elif [ "$L_TOT" -eq 0 ]; then
        echo "L_V4=$STAT_V4" > "$CACHE_DIR/.stats"; echo "L_V6=$STAT_V6" >> "$CACHE_DIR/.stats"
        echo "L_TOT=$STAT_TOTAL" >> "$CACHE_DIR/.stats"; echo "L_BYTES=$STAT_BYTES" >> "$CACHE_DIR/.stats"
        echo "L_TS='$SHOW_TS'" >> "$CACHE_DIR/.stats"
    fi
    if [ "$needs_update" -eq 1 ]; then apply_changes; else rm -f "$TMP_OUTPUT_FILE"; fi
}

main() {
    init_configuration "$@"
    setup_environment
    acquire_lock
    check_disk_space
    print_config_table
    phase_download
    phase_process
    phase_validate
    phase_sort
    phase_finalize
}

main "$@"
