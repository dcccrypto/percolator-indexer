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
