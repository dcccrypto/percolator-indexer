import sys,subprocess,re,hashlib,os
parts=re.split(r'^\\echo ==Q ',open(sys.argv[1]).read(),flags=re.M)[1:]
PSQL=[os.environ.get('PG','/opt/homebrew/opt/postgresql@16/bin')+'/psql','-h',os.environ.get('COV_DIR','/tmp/cov-bench'),'-p',os.environ.get('COV_PORT','55477'),'-U','postgres','-X','-q','-At','-v','ON_ERROR_STOP=1']
dbs=sys.argv[2:]; bad=0
for p in parts:
    lab,_,body=p.partition('\n'); body=body.strip().rstrip(';')
    if 'PLACEHOLDER' in body: continue
    body=body.replace(':EX::text[]',"'{slab1,slab2}'::text[]")
    # ORDER-insensitive compare: sort output lines (A6 DISTINCT ... LIMIT and ties have no defined order)
    h=[hashlib.md5('\n'.join(sorted(subprocess.run(PSQL+['-d',d,'-c',body],capture_output=True,text=True).stdout.splitlines())).encode()).hexdigest()[:10] for d in dbs]
    same=len(set(h))==1; bad+=not same
    print(('same ' if same else 'DIFF ')+lab[:60],h,flush=True)
print('diffs',bad)
