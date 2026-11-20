# table.py reads.json writes.tsv: markdown tables (min ms of 5 / buffers; S0 -> S1 -> S2)
import sys,json,csv,statistics as st
r=json.load(open(sys.argv[1]))
print('| Read | rows | S0 no covers: min ms / buf | S1 +20261115: min ms / buf | S2 +this PR: min ms / buf | S1->S2 | S2 plan (trades scan) |')
print('|---|---|---|---|---|---|---|')
for k,v in r.items():
    a,b,c=v['t0'],v['t1'],v['t2']
    f=lambda x: f"{x['min']:.2f} / {x['buf']}" if x.get('min') is not None else 'ERR'
    sp=f"{b['min']/c['min']:.1f}x" if c.get('min') else ''
    print(f"| {k} | {c.get('rows')} | {f(a)} | {f(b)} | {f(c)} | {sp} | {c.get('scan','')} |")
if len(sys.argv)>2:
    rows=list(csv.DictReader(open(sys.argv[2]),delimiter='\t'))
    print('\n| State | shape | rows/run | min secs | median secs | median rows/s | median WAL bytes | median Btree WAL | median Heap WAL | median FPI |')
    print('|---|---|---|---|---|---|---|---|---|---|')
    for s in ['t0','t1','t2']:
        for sh in ['single','batch50']:
            g=[x for x in rows if x['state']==s and x['shape']==sh]
            if not g: continue
            m=lambda c: st.median(float(x[c]) for x in g)
            n=int(g[0]['rows'])
            print(f"| {s} | {sh} | {n} | {min(float(x['secs']) for x in g):.2f} | {m('secs'):.2f} | {n/m('secs'):.0f} | {m('wal_bytes'):.0f} | {m('btree_bytes'):.0f} | {m('heap_bytes'):.0f} | {m('fpi_bytes'):.0f} |")
# optional: table.py reads.json writes.tsv writes-explain.tsv writes-cpu.tsv
if len(sys.argv)>3:
    rows=list(csv.DictReader(open(sys.argv[3]),delimiter='\t'))
    print('\n| State | mode | min exec ms (5) | WAL records | WAL FPI | WAL bytes | shared hit | hit/row |')
    print('|---|---|---|---|---|---|---|---|')
    for ck,lab in [('yes','after checkpoint'),('no','steady')]:
        for s in ['t0','t1','t2']:
            g=[x for x in rows if x['state']==s and x['checkpointed']==ck]
            m=lambda c: st.median(float(x[c]) for x in g)
            print(f"| {s} | {lab} | {min(float(x['exec_ms']) for x in g):.0f} | {m('wal_records'):.0f} | {m('wal_fpi'):.0f} | {m('wal_bytes'):.0f} | {m('shared_hit'):.0f} | {m('shared_hit')/10000:.1f} |")
if len(sys.argv)>4:
    rows=[x for x in csv.DictReader(open(sys.argv[4]),delimiter='\t') if x.get('user_s')]
    print('\n| State | runs | min user CPU s | median user CPU s | median user+sys s | median elapsed s | rows/CPU-s (median user+sys) |')
    print('|---|---|---|---|---|---|---|')
    for s in ['t0','t1','t2']:
        g=[x for x in rows if x['state']==s]
        u=[float(x['user_s']) for x in g]; us=[float(x['user_s'])+float(x['sys_s']) for x in g]
        print(f"| {s} | {len(g)} | {min(u):.3f} | {st.median(u):.3f} | {st.median(us):.3f} | {st.median(float(x['elapsed_s']) for x in g):.1f} | {10000/st.median(us):.0f} |")
