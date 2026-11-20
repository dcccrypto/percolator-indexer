# reads.py q.sql N out.json db1 db2 ...: per query, per rep, per db (interleaved) EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
import sys,subprocess,re,json,statistics,os
qf,N,out,dbs=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4:]
parts=re.split(r'^\\echo ==Q ',open(qf).read(),flags=re.M)[1:]
PSQL=[os.environ.get('PG','/opt/homebrew/opt/postgresql@16/bin')+'/psql','-h',os.environ.get('COV_DIR','/tmp/cov-bench'),'-p',os.environ.get('COV_PORT','55477'),'-U','postgres','-X','-q','-At','-v','ON_ERROR_STOP=1']
def scans(n,acc):
    if 'Relation Name' in n: acc.append(f"{n['Node Type']}{' using '+n['Index Name'] if 'Index Name' in n else ''} on {n['Relation Name']}"+(f" (heap fetches {n['Heap Fetches']})" if 'Heap Fetches' in n else ''))
    for c in n.get('Plans',[]): scans(c,acc)
    return acc
res={}
for p in parts:
    lab,_,body=p.partition('\n'); body=body.strip().rstrip(';')
    if 'PLACEHOLDER' in body: continue
    body=body.replace(':EX::text[]',"'{slab1,slab2}'::text[]")
    r={db:{'t':[]} for db in dbs}
    for i in range(N):
        for db in dbs:
            o=subprocess.run(PSQL+['-d',db,'-c','EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+body],capture_output=True,text=True)
            if o.returncode: r[db]['err']=o.stderr; continue
            j=json.loads(o.stdout)[0]; pl=j['Plan']
            r[db]['t'].append(j['Execution Time'])
            r[db]['buf']=pl.get('Shared Hit Blocks',0)+pl.get('Shared Read Blocks',0)
            r[db]['scan']='; '.join(dict.fromkeys(scans(pl,[])))
            r[db]['rows']=pl['Actual Rows']
    for db in dbs:
        t=sorted(r[db]['t']); r[db]['min']=t[0] if t else None; r[db]['med']=statistics.median(t) if t else None
    res[lab]=r
    print(lab[:50],' | '.join(f"{db} {r[db]['min']:.3f}/{r[db].get('buf')}" for db in dbs if r[db]['min'] is not None),flush=True)
    json.dump(res,open(out,'w'),indent=1)
