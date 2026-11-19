import csv, hashlib, json, os, re, shutil, subprocess, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin,urlsplit,urldefrag,quote
import requests
from bs4 import BeautifulSoup
from markdown_it import MarkdownIt
R=Path.cwd()
S=['cbailes/awesome-ai-cardiology','PierreElias/IntroECG','analysiscenter/cardio','mintisan/awesome-hrv','neuropsychology/NeuroKit','MIT-LCP/wfdb-python','awni/ecg']
EXT={'.txt','.md','.rst','.csv','.json','.yaml','.yml','.py','.ipynb','.html','.htm'}
for d in ['raw_repos','extracted_text','downloaded_links','pdfs','pdf_text','provenance','.cache']: (R/d).mkdir(exist_ok=True)
rows=[]; failures=[]; stats={}; manifests=[]; downloads=[]; pdfs=[]
def save(p,v): p.write_text(json.dumps(v,indent=2,ensure_ascii=False)+'\n')
def links(text,base):
    soup=BeautifulSoup(MarkdownIt('commonmark',{'html':True}).render(text),'html.parser')
    found=[(a.get_text(' ',strip=True),a.get('href') or a.get('src')) for a in soup.select('[href],[src]')]
    found += [('',u.rstrip('.,;\'")]>')) for u in re.findall(r'https?://[^\s<>"\x00-\x1f]+',text)]
    out={}
    for label,u in found:
        try:
            u=urljoin(base,u)
            p=urlsplit(u)
        except (ValueError,TypeError):
            continue
        if p.scheme in ('http','https') and p.hostname and not p.username and p.hostname not in ('localhost','127.0.0.1'):
            out.setdefault(u,label)
    return [(label,u) for u,label in out.items()]
def typ(u):
    p=urlsplit(u).path.lower()
    return 'PDF' if p.endswith('.pdf') or '/pdf/' in p or p.endswith('/pdf') else 'text' if p.endswith('.txt') else 'repo' if 'github.com' in u else 'paper' if any(x in u for x in ['doi.org','arxiv','pubmed']) else 'dataset' if 'physionet' in u else 'web'
def clone(s):
    n=s.split('/')[-1]; p=R/'raw_repos'/n
    if not p.exists(): subprocess.run(['git','clone','--depth','1','https://github.com/'+s+'.git',str(p)],check=True)
    subprocess.run(['git','-C',str(p),'submodule','update','--init','--depth','1'],check=True)
    sha=subprocess.check_output(['git','-C',str(p),'rev-parse','HEAD'],text=True).strip()
    save(R/'provenance'/f'{n}.json',{'repository':s,'commit':sha})
    return n,p,sha,s
with ThreadPoolExecutor(max_workers=7) as pool: sources=list(pool.map(clone,S))
local=set()
for n,p,sha,s in sources:
    stats[n]={'files':0,'text_files':0,'PDFs':0,'pdf_text_files':0,'links':0}
    with (R/'extracted_text'/f'{n}_all_text.txt').open('w',encoding='utf-8') as out:
        for f in sorted(p.rglob('*')):
            if not f.is_file() or '.git' in f.relative_to(p).parts: continue
            rel=f.relative_to(p).as_posix(); data=f.read_bytes();stats[n]['files']+=1
            manifests.append({'repo':n,'file':rel,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
            base=f'https://github.com/{s}/blob/{sha}/{quote(rel)}';local.add(urldefrag(base)[0])
            if f.suffix.lower()=='.pdf':
                dest=R/'pdfs'/n/'repository'/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(f,dest);pdfs.append((n,dest))
            if f.suffix.lower() not in EXT and not f.name.lower().startswith(('readme','license')):continue
            text=data.decode('utf-8',errors='replace')
            out.write('\n=== FILE: '+rel+' ===\n'+text+'\n');stats[n]['text_files']+=1
            if f.suffix.lower()=='.ipynb':
                try:text='\n'.join(''.join(c.get('source',[])) for c in json.loads(text)['cells'])
                except Exception:pass
            if f.suffix.lower()=='.csv':continue
            for label,u in links(text,base):rows.append(dict(source_repo=n,source_file=rel,link_text=label,url=u,type=typ(u)))
    print('Extracted',n,stats[n],flush=True)
def fetch(u):
    key=hashlib.sha256(u.encode()).hexdigest(); body=R/'.cache'/key
    target=u
    if target.startswith('http:'):target='https:'+target[5:]
    if 'github.com/' in target and '/blob/' in target:target=target.replace('github.com/','raw.githubusercontent.com/').replace('/blob/','/')
    try:
        with requests.get(target,timeout=(10,25),stream=True,headers={'User-Agent':'dil-ka-darya-research-collector'}) as res:
            res.raise_for_status(); ct=res.headers.get('content-type','');start=time.monotonic();size=0
            with body.open('wb') as out:
                for chunk in res.iter_content(131072):
                    size+=len(chunk);out.write(chunk)
                    if time.monotonic()-start>90 or ('html' in ct and size>10000000):raise ValueError('Transfer limit exceeded')
            sig=body.open('rb').read(1024)
            kind='PDF' if b'%PDF-' in sig else 'text' if 'text/plain' in ct else 'html' if 'html' in ct else 'other'
            return dict(ok=True,url=u,final_url=res.url,body=body,kind=kind)
    except Exception as e:return dict(ok=False,url=u,error=str(e))
seen=set(); cache={}
def phase(groups,expand):
    with ThreadPoolExecutor(max_workers=24) as pool:
        todo={pool.submit(fetch,u):u for u in groups if u not in cache}
        for i,f in enumerate(as_completed(todo),1):
            result=f.result();cache[result['url']]=result
            if i%50==0:print('Fetched',i,'/',len(todo),flush=True)
    new=[]
    for u,origins in groups.items():
        result=cache[u]
        if not result['ok']:
            for r in origins:failures.append(dict(r,error=result['error']))
            continue
        if typ(u)=='PDF' and result['kind']!='PDF':
            for r in origins:failures.append(dict(r,error='PDF link returned non-PDF content'))
        if result['kind'] in ('PDF','text'):
            for n in {r['source_repo'] for r in origins}:
                if (n,u) in seen:continue
                seen.add((n,u));ext='.pdf' if result['kind']=='PDF' else '.txt'
                name=hashlib.sha256(u.encode()).hexdigest()[:16]+ext
                dest=R/'downloaded_links'/n/name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(result['body'],dest)
                downloads.append(dict(repo=n,url=u,final_url=result['final_url'],file=str(dest.relative_to(R)),sha256=hashlib.sha256(dest.read_bytes()).hexdigest()))
                if ext=='.pdf':
                    pdf=R/'pdfs'/n/'downloaded'/name;pdf.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(dest,pdf);pdfs.append((n,pdf))
        elif expand and result['kind']=='html':
            found=links(result['body'].read_text(errors='replace'),result['final_url'])
            for n in {r['source_repo'] for r in origins}:
                for label,child in found:new.append(dict(source_repo=n,source_file='[one-hop] '+u,link_text=label,url=child,type=typ(child)))
    return new
groups={}
skip={'.png','.jpg','.jpeg','.svg','.gif','.zip','.gz','.mp4','.csv','.npy','.mat','.dat','.wav','.ico','.whl'}
for r in rows:
    u=urldefrag(r['url'])[0]
    if u not in local and Path(urlsplit(u).path).suffix.lower() not in skip:groups.setdefault(u,[]).append(r)
print('First-level URLs',len(groups),flush=True)
children=phase(groups,True);rows+=children;groups={}
for r in children:
    u=urldefrag(r['url'])[0]
    if r['type'] in ('PDF','text') and u not in local:groups.setdefault(u,[]).append(r)
print('Document sublinks',len(groups),flush=True);phase(groups,False)
errors=[]
for n,pdf in pdfs:
    stats[n]['PDFs']+=1
    dest=R/'pdf_text'/pdf.relative_to(R/'pdfs').with_suffix('.txt');dest.parent.mkdir(parents=True,exist_ok=True)
    r=subprocess.run(['pdftotext','-layout','-enc','UTF-8',str(pdf),str(dest)],capture_output=True,text=True,timeout=120)
    if r.returncode or not dest.exists() or not dest.read_text().strip():errors.append(dict(file=str(pdf.relative_to(R)),error=r.stderr or 'No embedded text; OCR required'))
    else:stats[n]['pdf_text_files']+=1
fields=['source_repo','source_file','link_text','url','type']
rows=list({tuple(r[k] for k in fields):r for r in rows}.values())
with (R/'links_index.csv').open('w',newline='',encoding='utf-8') as f:
    w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
with (R/'links_index.md').open('w',encoding='utf-8') as f:
    f.write('# Links index\n\n| '+' | '.join(fields)+' |\n| '+' | '.join(['---']*5)+' |\n')
    for r in rows:f.write('| '+' | '.join(r[k].replace('|','&#124;').replace('\n',' ') for k in fields)+' |\n')
for n in stats:stats[n]['links']=sum(r['source_repo']==n for r in rows)
save(R/'provenance/source_files.json',manifests);save(R/'provenance/downloads.json',downloads)
(R/'failed_links.log').write_text(''.join(json.dumps(x)+'\n' for x in failures))
(R/'extraction_errors.log').write_text(''.join(json.dumps(x)+'\n' for x in errors))
# Remove only clone metadata; source files and notices remain unchanged.
for p in sorted((R/'raw_repos').rglob('.git'),key=lambda p:len(p.parts),reverse=True):
    if p.is_dir():shutil.rmtree(p)
    else:p.unlink()
parts=[]
for p in (R/'extracted_text').glob('*_all_text.txt'):
    if p.stat().st_size<=95000000:continue
    names=[]
    with p.open('rb') as f:
        i=0
        while True:
            b=f.read(70000000)
            if not b:break
            i+=1;q=p.with_name(p.stem+f'.part{i:03d}');q.write_bytes(b);names.append(q.name)
    parts.append(dict(file=p.name,parts=names))
save(R/'provenance/text_parts.json',parts)
large=[]
for p in R.rglob('*'):
    if p.is_file() and not any(x in p.relative_to(R).parts for x in ['.git','.cache']):
        if p.stat().st_size>95000000:large.append(dict(file=p.relative_to(R).as_posix(),bytes=p.stat().st_size))
(R/'large_files_skipped.log').write_text(''.join(json.dumps(x)+'\n' for x in large))
(R/'.gitignore').write_text('.DS_Store\n__pycache__/\n.cache/\ncollection.log\n'+''.join('/'+x['file']+'\n' for x in large))
report=dict(repositories=stats,failed_unique_urls=len({x['url'] for x in failures}),pdf_extraction_errors=len(errors),large_files=large)
save(R/'collection_report.json',report)
table='| Repository | Original files | Text files | PDFs | Link rows |\n|---|---:|---:|---:|---:|\n'
for n,s in stats.items():table+=f"| {n} | {s['files']} | {s['text_files']} | {s['PDFs']} | {s['links']} |\n"
(R/'README.md').write_text('# Dil ka darya\n\n'+table+'\nOriginal snapshots: raw_repos/. Searchable dumps: extracted_text/. Downloaded documents: downloaded_links/. PDFs: pdfs/. PDF text: pdf_text/. Links: links_index.csv and links_index.md. Provenance and checksums: provenance/. Failures: failed_links.log and extraction_errors.log. Size exclusions: large_files_skipped.log.\n\nExternal links are followed one level only. Rows include links discovered on external pages; not all indexed links are fetched. Network failures, access restrictions and malformed source URLs are logged. Image-only PDFs are flagged for OCR. Upstream licenses and notices remain in source folders; no additional rights are granted.\n\nOn your Mac:\n\n```sh\ngit clone https://github.com/adhyanshbhandari0-ui/dil-ka-darya.git ~/dil-ka-darya\npython3 ~/dil-ka-darya/restore_text_dumps.py\n```\n\nFiles over 95 MB are excluded from Git. Oversized text dumps can be reconstructed exactly from their committed parts.\n')
paths=[]
for p in R.rglob('*'):
    rel=p.relative_to(R)
    if not p.is_file() or any(x in rel.parts for x in ['.git','.cache','__pycache__']) or p.name in ['.DS_Store','collection.log'] or p.stat().st_size>95000000:continue
    paths.append(rel.as_posix())
(R/'.git/info/attributes').write_text('* -text -filter -ident -working-tree-encoding\n')
subprocess.run(['git','add','-f','--pathspec-from-file=-','--pathspec-file-nul'],input=('\0'.join(paths)+'\0').encode(),check=True)
print(json.dumps(report,indent=2),flush=True)
