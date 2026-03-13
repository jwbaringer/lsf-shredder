#!/usr/bin/env python3
"""
LSF lsb.acct parser — LSF 10 format.
Outputs all parsed fields to stdout for inspection.
"""

import sys
import shlex

# Fixed fields 0-21 (before variable host sections)
FIELDS_PRE = [
    'event_type',        # 0
    'version_number',    # 1
    'event_time',        # 2
    'job_id',            # 3
    'user_id',           # 4
    'options',           # 5
    'num_processors',    # 6
    'submit_time',       # 7
    'begin_time',        # 8
    'term_time',         # 9
    'start_time',        # 10
    'user_name',         # 11
    'queue',             # 12
    'res_req',           # 13
    'depend_cond',       # 14
    'pre_exec_cmd',      # 15
    'from_host',         # 16
    'cwd',               # 17
    'in_file',           # 18
    'out_file',          # 19
    'err_file',          # 20
    'job_file',          # 21
]

# After variable host sections: fixed fields through job_description
FIELDS_MIDDLE = [
    'j_status',
    'host_factor',
    'job_name',
    'command',
    'ru_utime',
    'ru_stime',
    'ru_maxrss',
    'ru_ixrss',
    'ru_ismrss',
    'ru_idrss',
    'ru_isrss',
    'ru_minflt',
    'ru_majflt',
    'ru_nswap',
    'ru_inblock',
    'ru_oublock',
    'ru_ioch',
    'ru_msgsnd',
    'ru_msgrcv',
    'ru_nsignals',
    'ru_nvcsw',
    'ru_nivcsw',
    'ru_exutime',
    'mail_user',
    'project_name',
    'exit_status',
    'max_num_processors',
    'login_shell',
    'time_event',
    'idx',
    'max_rmem',
    'max_rswap',
    'in_file_spool',
    'command_spool',
    'rsv_id',
    'sla',
    'except_mask',
    'additional_info',
    'exit_info',
    'warning_action',
    'warning_time_period',
    'charged_saap',
    'license_project',
    'app',
    'post_exec_cmd',
    'runtime_estimation',
    'job_group_name',
    'requeue_evalues',
    'options2',
    'resize_notify_cmd',
    'last_resize_time',
    'rsv_id2',
    'job_description',
]

# Known submit_ext tag IDs
SUBMIT_EXT_TAGS = {
    '1032': 'ext_inelig_pend_time',
    '1033': 'ext_elig_pend_time',
    '1041': 'ext_app_profile',
    '1060': 'ext_project',
    '1086': 'ext_cpu_time_vector',
    '1103': 'ext_1103',
    '1105': 'ext_1105',
    '1106': 'ext_1106',
    '1110': 'ext_default_profile',
    '1111': 'ext_1111',
    '1112': 'ext_1112',
    '1113': 'ext_1113',
    '1042': 'ext_gpu_req',
}


def _get(fields, idx, name):
    if idx >= len(fields):
        raise IndexError(f'Field {name!r} expected at index {idx} but only {len(fields)} fields')
    return fields[idx], idx + 1


def parse_line(line):
    try:
        fields = shlex.split(line)
    except ValueError as e:
        print(f'  SHLEX ERROR: {e}', file=sys.stderr)
        return None

    job = {}
    idx = 0

    # --- fixed pre-host fields (0-21) ---
    for name in FIELDS_PRE:
        job[name], idx = _get(fields, idx, name)

    # --- variable: asked hosts ---
    num_asked = int(fields[idx]); idx += 1
    job['num_asked_hosts'] = num_asked
    job['asked_hosts'] = fields[idx:idx + num_asked]
    idx += num_asked

    # --- variable: exec hosts ---
    num_exec = int(fields[idx]); idx += 1
    job['num_ex_hosts'] = num_exec
    raw_hosts = fields[idx:idx + num_exec]
    # strip leading "N*" slot prefix e.g. "4*n101" -> "n101"
    job['exec_hosts'] = [h.split('*', 1)[-1] for h in raw_hosts]
    idx += num_exec

    # --- fixed middle fields (j_status ... job_description) ---
    for name in FIELDS_MIDDLE:
        job[name], idx = _get(fields, idx, name)

    # --- submit_ext: count + N tag-value pairs ---
    submit_ext_count = int(fields[idx]); idx += 1
    job['submit_ext_count'] = submit_ext_count
    for _ in range(submit_ext_count):
        tag = fields[idx]; idx += 1
        val = fields[idx]; idx += 1
        name = SUBMIT_EXT_TAGS.get(tag, f'ext_{tag}')
        job[name] = val

    # --- num_host_rusage + entries (hostname, mem, swap, utime, stime) ---
    num_host_rusage = int(fields[idx]); idx += 1
    job['num_host_rusage'] = num_host_rusage
    rusage_entries = []
    for i in range(num_host_rusage):
        entry = {
            'hostname': fields[idx],
            'mem':      fields[idx+1],
            'swap':     fields[idx+2],
            'utime':    fields[idx+3],
            'stime':    fields[idx+4],
        }
        rusage_entries.append(entry)
        idx += 5
    job['host_rusage'] = rusage_entries

    # --- fixed post-rusage fields ---
    for name in ['options3', 'run_limit', 'avg_mem', 'effective_res_req',
                 'src_cluster', 'src_job_id', 'dst_cluster', 'dst_job_id',
                 'forward_time', 'flow_id', 'ac_job_wait_time',
                 'total_provision_time', 'outdir', 'run_time', 'subcwd']:
        job[name], idx = _get(fields, idx, name)

    # --- num_network + conditional network alloc + affinity ---
    num_network = int(fields[idx]); idx += 1
    job['num_network'] = num_network
    networks = []
    for i in range(num_network):
        networks.append({'network_id': fields[idx], 'num_window': fields[idx+1]})
        idx += 2
    job['networks'] = networks
    # affinity is always logged (empty string if none)
    job['affinity'], idx = _get(fields, idx, 'affinity')

    # --- performance counters ---
    for name in ['serial_job_energy', 'cpi', 'gips', 'gbs', 'gflops']:
        job[name], idx = _get(fields, idx, name)

    # --- alloc slots (num + list) ---
    num_alloc_slots = int(fields[idx]); idx += 1
    job['num_alloc_slots'] = num_alloc_slots
    job['alloc_slots'] = fields[idx:idx + num_alloc_slots]
    idx += num_alloc_slots

    # --- pend time / requeue ---
    job['ineligible_pend_time'], idx = _get(fields, idx, 'ineligible_pend_time')
    index_range_cnt = int(fields[idx]); idx += 1
    job['index_range_cnt'] = index_range_cnt
    ranges = []
    for _ in range(index_range_cnt):
        ranges.append({'start': fields[idx], 'end': fields[idx+1], 'step': fields[idx+2]})
        idx += 3
    job['index_ranges'] = ranges
    job['requeue_time'], idx = _get(fields, idx, 'requeue_time')

    # --- GPU rusage (num + per-host entries) ---
    num_gpu_rusages = int(fields[idx]); idx += 1
    job['num_gpu_rusages'] = num_gpu_rusages
    gpu_rusages = []
    for _ in range(num_gpu_rusages):
        hostname = fields[idx]; idx += 1
        num_kvp = int(fields[idx]); idx += 1
        kvps = {}
        for _ in range(num_kvp):
            k = fields[idx]; idx += 1
            v = fields[idx]; idx += 1
            kvps[k] = v
        gpu_rusages.append({'hostname': hostname, 'kvp': kvps})
    job['gpu_rusages'] = gpu_rusages

    # --- storage info ---
    storage_info_c = int(fields[idx]); idx += 1
    job['storage_info_c'] = storage_info_c
    storage_entries = []
    for _ in range(storage_info_c):
        storage_entries.append(fields[idx]); idx += 1
    job['storage_info'] = storage_entries

    # --- finishKVP ---
    finish_kvp_count = int(fields[idx]); idx += 1
    job['finish_kvp_count'] = finish_kvp_count
    finish_kvp = {}
    for _ in range(finish_kvp_count):
        k = fields[idx]; idx += 1
        v = fields[idx]; idx += 1
        finish_kvp[k] = v
    job['finish_kvp'] = finish_kvp

    # --- scheduling overhead ---
    if idx < len(fields):
        job['scheduling_overhead'], idx = _get(fields, idx, 'scheduling_overhead')

    # --- anything left over ---
    if idx < len(fields):
        job['_extra'] = fields[idx:]

    return job


def print_job(job, line_num):
    print(f'\n{"="*60}')
    print(f'LINE {line_num}')
    print(f'{"="*60}')
    for k, v in job.items():
        print(f'  {k:30s} = {v}')


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else 'lsb.acct'

    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            if not line.startswith('"JOB_FINISH"') and not line.startswith('JOB_FINISH'):
                print(f'[line {line_num}] skipping: {line[:60]}')
                continue
            try:
                job = parse_line(line)
            except Exception as e:
                print(f'[line {line_num}] PARSE ERROR: {e}', file=sys.stderr)
                continue
            if job:
                print_job(job, line_num)


if __name__ == '__main__':
    main()
