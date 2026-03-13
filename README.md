# lsf-shredder

Parses IBM LSF 10 `lsb.acct` accounting logs and loads job records into a PostgreSQL database.

## Files

- **shredder.py** — standalone parser that prints all fields to stdout; useful for inspecting and validating `lsb.acct` records
- **acctdb_master.py** — production parser; reads `lsb.acct`, computes service units, and inserts records into the `jobs` table
- **Lsf.php** — reference LSF 9 shredder (OpenXdmod)
- **acctdb.py** — earlier LSF 9 prototype

## LSF 10 format notes

Records are space-separated with shell quoting. Key variable-length sections:

- `num_asked_hosts` + N host names
- `num_ex_hosts` + N host names
- `submit_ext_count` + N tag-value pairs (tags 1032, 1033, 1041, 1060, 1086, 1110, etc.)
- `num_host_rusage` + N entries (hostname, mem, swap, utime, stime)
- `num_network` + N network allocations
- `num_alloc_slots` + N slot host names
- `num_gpu_rusages` + per-host GPU KVP entries
- `finishKVP` + N key-value pairs (includes `schedulingOverhead`)

## Usage

```bash
# Inspect a file
python3 shredder.py lsb.acct

# Load into database
python3 acctdb_master.py --file lsb.acct [--lookup-project] [--debug]
```

## Service units

`total_service_units = ceil(cpu_hours) * max(1.0, mem_GB / 1.8)`
