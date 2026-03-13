#!/usr/bin/env python3
import sys
import os
import math
import shlex
import argparse
import time
import signal
import faulthandler
import subprocess
from datetime import datetime

import psycopg2 as pg
import dbconn

# -----------------------------
# Tuning knobs
# -----------------------------
# Max *raw bytes* allowed for a single line in the input file.
MAX_LINE_BYTES = 5 * 1024 * 1024      # 5 MB (generous). Consider 1MB once stable.

# Chunk size for binary reading
READ_CHUNK_BYTES = 8 * 1024 * 1024    # 8 MB

SLOW_PARSE_SEC = 2.0                  # Log if parsing a single line takes longer than this
MIN_FIELDS = 40                       # Minimum expected fields after splitting
PROJECT_LOOKUP_TIMEOUT = 2            # Seconds
PG_STATEMENT_TIMEOUT = "30s"          # Set to None to disable

# Enable: kill -USR1 <pid> to dump Python stack traces
faulthandler.register(signal.SIGUSR1)

_project_cache = {}


def get_service_unit(cpusecs, mem):
    def cpuhour_unit(cpusecs):
        return int(math.ceil(cpusecs / 3600))

    def mem_factor(mem, mem_unit_size=1.8):
        if mem is None or mem <= 0:
            mem = 1.0
        unit = float(mem / 1024.0 / 1024.0 / mem_unit_size)
        return max(1.0, unit)

    fact = mem_factor(mem)
    return cpuhour_unit(cpusecs) * fact


def safe_lookup_project(username: str, timeout_s: int = PROJECT_LOOKUP_TIMEOUT) -> str:
    if username in _project_cache:
        return _project_cache[username]

    proj = "default"
    try:
        r = subprocess.run(
            ["./userproject.sh", username],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        out = (r.stdout or "").strip()
        if out:
            proj = out
    except subprocess.TimeoutExpired:
        proj = "default"
    except Exception:
        proj = "default"

    _project_cache[username] = proj
    return proj


def iter_lines_binary(path: str, save_bad_dir: str | None, debug: bool):
    """
    Robust line iterator:
      - reads in binary chunks
      - splits on b'\\n'
      - skips and optionally saves lines exceeding MAX_LINE_BYTES
      - yields (line_no, decoded_line_str)
    """
    buf = b""
    line_no = 0
    skipped = 0

    if save_bad_dir:
        os.makedirs(save_bad_dir, exist_ok=True)

    with open(path, "rb") as f:
        while True:
            chunk = f.read(READ_CHUNK_BYTES)
            if not chunk:
                # flush remainder (last line may not end with \n)
                if buf:
                    line_no += 1
                    if len(buf) > MAX_LINE_BYTES:
                        skipped += 1
                        if debug:
                            print(f"DEBUG: SKIP monster final line {line_no}: {len(buf)} bytes", flush=True)
                        if save_bad_dir:
                            outpath = os.path.join(save_bad_dir, f"monster_line_{line_no}.bin")
                            with open(outpath, "wb") as out:
                                out.write(buf[:MAX_LINE_BYTES])
                        buf = b""
                    else:
                        yield line_no, buf.decode("utf-8", errors="ignore")
                if debug and skipped:
                    print(f"DEBUG: Total skipped monster lines: {skipped}", flush=True)
                break

            buf += chunk

            # If buffer grows huge without *any* newline, it's a monster line
            if len(buf) > MAX_LINE_BYTES and b"\n" not in buf:
                line_no += 1
                skipped += 1
                if debug:
                    print(f"DEBUG: SKIP monster line {line_no}: >{MAX_LINE_BYTES} bytes without newline", flush=True)
                if save_bad_dir:
                    outpath = os.path.join(save_bad_dir, f"monster_line_{line_no}.bin")
                    with open(outpath, "wb") as out:
                        out.write(buf[:MAX_LINE_BYTES])
                buf = b""
                continue

            parts = buf.split(b"\n")
            buf = parts.pop()  # remainder

            for p in parts:
                line_no += 1
                if len(p) > MAX_LINE_BYTES:
                    skipped += 1
                    if debug:
                        print(f"DEBUG: SKIP long line {line_no}: {len(p)} bytes", flush=True)
                    if save_bad_dir:
                        outpath = os.path.join(save_bad_dir, f"long_line_{line_no}.bin")
                        with open(outpath, "wb") as out:
                            out.write(p[:MAX_LINE_BYTES])
                    continue

                yield line_no, p.decode("utf-8", errors="ignore")


def _split_line(line: str, debug: bool, line_no: int | None):
    # Fast-path: if no quotes/backslashes, avoid shlex entirely
    if '"' not in line and "'" not in line and "\\" not in line:
        rec = line.split()
    else:
        try:
            rec = shlex.split(line)
        except ValueError as e:
            if debug:
                print(f"DEBUG: SKIP - shlex error line {line_no}: {e}", flush=True)
            return None

    if len(rec) < MIN_FIELDS:
        if debug:
            print(f"DEBUG: SKIP - too few fields line {line_no}: {len(rec)}", flush=True)
        return None

    return rec


# LSF 10 sequential field parser
# Fixed fields 0-21 (before variable host sections)
_FIELDS_PRE = [
    'event_type', 'version_number', 'event_time', 'job_id', 'user_id',
    'options', 'num_processors', 'submit_time', 'begin_time', 'term_time',
    'start_time', 'user_name', 'queue', 'res_req', 'depend_cond',
    'pre_exec_cmd', 'from_host', 'cwd', 'in_file', 'out_file',
    'err_file', 'job_file',
]

# Fixed fields from j_status through job_description
_FIELDS_MIDDLE = [
    'j_status', 'host_factor', 'job_name', 'command',
    'ru_utime', 'ru_stime', 'ru_maxrss', 'ru_ixrss', 'ru_ismrss',
    'ru_idrss', 'ru_isrss', 'ru_minflt', 'ru_majflt', 'ru_nswap',
    'ru_inblock', 'ru_oublock', 'ru_ioch', 'ru_msgsnd', 'ru_msgrcv',
    'ru_nsignals', 'ru_nvcsw', 'ru_nivcsw', 'ru_exutime',
    'mail_user', 'project_name', 'exit_status', 'max_num_processors',
    'login_shell', 'time_event', 'idx', 'max_rmem', 'max_rswap',
    'in_file_spool', 'command_spool', 'rsv_id', 'sla', 'except_mask',
    'additional_info', 'exit_info', 'warning_action', 'warning_time_period',
    'charged_saap', 'license_project', 'app', 'post_exec_cmd',
    'runtime_estimation', 'job_group_name', 'requeue_evalues', 'options2',
    'resize_notify_cmd', 'last_resize_time', 'rsv_id2', 'job_description',
]


def parse_line(line: str, use_project_lookup=False, debug=False, line_no=None):
    job_info = {
        "job_id": "",
        "user_id": "",
        "start_time_epoch": 0,
        "secs": 0.0,
        "uid": 0,
        "mem": 0.0,
        "nhosts": 0,
        "status": 0,
        "stime": 0.0,
        "utime": 0.0,
        "project": "",
        "queue": "",
    }

    t0 = time.perf_counter()
    rec = _split_line(line, debug=debug, line_no=line_no)
    t1 = time.perf_counter()

    if debug and (t1 - t0) > 1.0:
        print(f"DEBUG: split slow {t1-t0:.2f}s line_no={line_no} len={len(line)}", flush=True)

    if rec is None:
        return job_info

    try:
        idx = 0

        # --- fixed pre fields (event_type .. job_file) ---
        pre = {}
        for name in _FIELDS_PRE:
            pre[name] = rec[idx]; idx += 1

        job_info["job_id"]            = pre['job_id']
        job_info["user_id"]           = pre['user_name']
        job_info["uid"]               = int(pre['user_id'])
        job_info["queue"]             = pre['queue']
        job_info["start_time_epoch"]  = int(pre['start_time'])

        # --- variable: asked hosts ---
        num_asked = int(rec[idx]); idx += 1
        idx += num_asked                          # skip asked host names

        # --- variable: exec hosts ---
        num_exec = int(rec[idx]); idx += 1
        job_info["nhosts"] = num_exec
        idx += num_exec                           # skip exec host names

        # --- fixed middle fields (j_status .. job_description) ---
        mid = {}
        for name in _FIELDS_MIDDLE:
            mid[name] = rec[idx]; idx += 1

        job_info["status"] = int(mid['j_status'])

        # Skip jobs that didn't actually run
        if job_info["status"] not in (32, 64) or job_info["nhosts"] == 0:
            return job_info

        job_info["utime"]   = float(mid['ru_utime'])
        job_info["stime"]   = float(mid['ru_stime'])
        job_info["mem"]     = int(mid['max_rmem'])
        job_info["project"] = mid['project_name']

        # --- submit_ext: count + N tag-value pairs ---
        submit_ext_count = int(rec[idx]); idx += 1
        idx += submit_ext_count * 2               # skip all tag-value pairs

        # --- num_host_rusage + entries (hostname, mem, swap, utime, stime) ---
        num_host_rusage = int(rec[idx]); idx += 1
        idx += num_host_rusage * 5                # skip each entry's 5 fields

        # --- options3, run_limit, avg_mem, effective_res_req ... ---
        # (remaining fields not needed for DB insertion — stop here)

        if use_project_lookup:
            if not job_info["project"] or job_info["project"] == "default":
                job_info["project"] = safe_lookup_project(job_info["user_id"])

    except Exception as e:
        if debug:
            print(f"DEBUG: Parse Error line_no={line_no}: {e}", flush=True)
    finally:
        if job_info["utime"] > 0:
            job_info["secs"] += job_info["utime"]
        if job_info["stime"] > 0:
            job_info["secs"] += job_info["stime"]

    return job_info


def main():
    parser = argparse.ArgumentParser(description="Parse LSF accounting logs into SQL.")
    parser.add_argument("--file", required=True, help="Path to the filtered LSF accounting file")
    parser.add_argument("--config", default=None, help="Database INI config file (optional)")
    parser.add_argument("--lookup-project", action="store_true", help="Enable external project lookup script")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    parser.add_argument("--save-bad-lines", action="store_true",
                        help="Save skipped monster lines to bad_lines/ as .bin samples")
    args = parser.parse_args()

    conn = dbconn.connect(args.config) if args.config else dbconn.connect()

    tsecs = 0.0
    tsu = 0.0
    inserted_count = 0

    print(f"Processing: {args.file}")
    if args.debug:
        print("DEBUG MODE ON")
        print(f"MAX_LINE_BYTES={MAX_LINE_BYTES}, READ_CHUNK_BYTES={READ_CHUNK_BYTES}, MIN_FIELDS={MIN_FIELDS}")

    bad_dir = "bad_lines" if args.save_bad_lines else None

    cur = conn.cursor()

    if PG_STATEMENT_TIMEOUT:
        try:
            cur.execute(f"SET statement_timeout = '{PG_STATEMENT_TIMEOUT}'")
        except Exception as e:
            print(f"Warning: could not set statement_timeout: {e}")

    # Robust iteration
    for line_no, line in iter_lines_binary(args.file, save_bad_dir=bad_dir, debug=args.debug):
        # Progress heartbeat by line_no
        if not args.debug and line_no % 5000 == 0:
            print(f"  ...processed {line_no}...", flush=True)

        t0 = time.perf_counter()
        job = parse_line(line, use_project_lookup=args.lookup_project, debug=args.debug, line_no=line_no)
        dt = time.perf_counter() - t0

        if dt > SLOW_PARSE_SEC:
            snippet = line[:200].replace("\n", "\\n").replace("\r", "\\r")
            print(f"\nSLOW parse line {line_no}: {dt:.2f}s len={len(line)} snippet={snippet}", flush=True)

        if not job["job_id"]:
            continue

        mem_su = job["mem"] / job["nhosts"] if job["nhosts"] else 0
        cpu_su = job["secs"] / 3600
        sunits = get_service_unit(job["secs"], mem_su)

        tsu += sunits
        tsecs += job["secs"]

        job["start_time"] = datetime.fromtimestamp(job["start_time_epoch"])
        job["cpu_service_units"] = int(cpu_su)
        job["total_service_units"] = int(sunits)

        if job["secs"] <= 1:
            continue

        columns = list(job.keys())
        placeholders = ",".join(["%s"] * len(columns))
        insert_statement = f"INSERT INTO jobs ({','.join(columns)}) VALUES ({placeholders})"
        values = [job[c] for c in columns]

        try:
            cur.execute(insert_statement, values)
            inserted_count += 1
        except (Exception, pg.DatabaseError) as error:
            print(f"DB Error (line {line_no}, job {job.get('job_id','')}): {error}")

    conn.commit()
    cur.close()

    print("\nSummary:")
    # line_no here is last line number seen in iter_lines_binary; if file empty, set to 0
    print(f"Total Inserted: {inserted_count}")
    conn.close()


if __name__ == "__main__":
    main()
