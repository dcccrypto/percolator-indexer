#!/usr/bin/env python3
"""Run explain.sql N times; print min/median execution time and the top-node shared buffers per query.
usage: best.py <psql-command> [runs]   (e.g. best.py "psql -h /tmp -p 55433 bench" 15)"""
import re, shlex, statistics, subprocess, sys, os

cmd = shlex.split(sys.argv[1])
runs = int(sys.argv[2]) if len(sys.argv) > 2 else 15
sql = os.path.join(os.path.dirname(os.path.abspath(__file__)), "explain.sql")
times, bufs = {}, {}
for _ in range(runs):
    out = subprocess.run(cmd + ["-q", "-f", sql], capture_output=True, text=True).stdout
    q = None
    for line in out.splitlines():
        if re.fullmatch(r"Q\d", line.strip()):
            q = line.strip()
        m = re.search(r"Execution Time: ([\d.]+) ms", line)
        if m and q: times.setdefault(q, []).append(float(m.group(1)))
        m = re.match(r"\s*Buffers: shared (.*)", line)
        if m and q and q not in bufs: bufs[q] = m.group(1)  # first Buffers line = top node total
for q in sorted(times):
    t = times[q]
    print(f"{q}: min={min(t):.2f}ms median={statistics.median(t):.2f}ms buffers[{bufs.get(q)}] runs={len(t)}")
