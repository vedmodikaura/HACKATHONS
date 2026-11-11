import re,os,json,math,random,sqlite3,calendar,hmac,hashlib,statistics,csv,glob,time,base64,shutil,subprocess,itertools,warnings,urllib.parse
from datetime import date,datetime,timedelta
ENV={}
def env(k,d=None):
    if not ENV:
        ENV['_']='1'
        p=os.environ.get('PIPELINE_ENV','.env')
        if os.path.exists(p):
            for l in open(p,encoding='utf-8'):
                l=l.strip()
                if l and not l.startswith('#') and '=' in l:
                    a,b=l.split('=',1)
                    ENV[a.strip()]=b.strip().strip('"').strip("'")
    v=os.environ.get(k,ENV.get(k))
    return d if v in (None,'') else v
def when(s):
    s=re.sub(r'(\d)(st|nd|rd|th)\b',r'\1',str(s or '').strip())
    for f in ('%d/%m/%Y','%d %B %Y','%d %b %Y','%Y-%m-%d','%d.%m.%Y','%d-%m-%Y','%a %d %b %Y','%A %d %B %Y','%d/%m/%y'):
        try:return datetime.strptime(s,f).date().isoformat()
        except ValueError:pass
    m=re.match(r'(\d{4}-\d{2}-\d{2})',s)
    return m.group(1) if m else None
class Tools:
    def tool(self,name,make):
        if name not in self.__dict__:self.__dict__[name]=make()
        return self.__dict__[name]
    def jev(self):return self.tool('_jev',Jev)
    def llm(self):return self.tool('_llm',Llm)
class Web:
    def __init__(self,root='raw',delay=None,transport=None):
        import httpx
        self.root,self.last=os.path.abspath(root),0.0
        os.makedirs(self.root,exist_ok=True)
        self.delay=float(env('WEB_DELAY','1') if delay is None else delay)
        self.client=httpx.Client(follow_redirects=True,timeout=float(env('WEB_TIMEOUT','30')),headers={'User-Agent':env('USER_AGENT','refusals-pipeline/1.0')},transport=transport)
    def wait(self):
        t=self.delay-(time.monotonic()-self.last)
        if t>0:time.sleep(t)
        self.last=time.monotonic()
    def get(self,url,**kw):
        self.wait()
        r=self.client.get(url,**kw)
        r.raise_for_status()
        return r
    def post(self,url,**kw):
        self.wait()
        r=self.client.post(url,**kw)
        r.raise_for_status()
        return r
    def keep(self,content,ext):
        p=os.path.join(self.root,hashlib.sha256(content).hexdigest()+ext)
        if not os.path.exists(p):
            with open(p,'wb') as f:f.write(content)
        return p
    def file(self,url):
        r=self.get(url)
        ext='.pdf' if 'pdf' in r.headers.get('content-type','') or url.lower().split('?')[0].endswith('.pdf') else os.path.splitext(urllib.parse.urlparse(url).path)[1] or '.bin'
        return self.keep(r.content,ext)
class Jev:
    def __init__(self,transport=None):
        import httpx
        self.url,self.model=env('JEV_URL','https://openrouter.ai/api/alpha/decisions'),env('JEV_MODEL','typesafe/jev-1.14')
        self.threshold,self.chars=float(env('JEV_THRESHOLD','0.9')),int(env('JEV_STATE_CHARS','100000'))
        self.client=httpx.Client(timeout=120,headers={'Authorization':'Bearer '+env('JEV_API_KEY',''),'Content-Type':'application/json'},transport=transport)
    @staticmethod
    def choice(instructions,criteria):return {'type':'choice','instructions':instructions,'criteria':criteria}
    @staticmethod
    def noul(instructions,true,false):return {'type':'noul','instructions':instructions,'criteria':{'true':true,'false':false}}
    @staticmethod
    def score(instructions,levels):return {'type':'score','instructions':instructions,'criteria':list(levels)}
    def ask(self,state,questions):
        if isinstance(state,str):state=state[:self.chars]
        r=self.client.post(self.url,json={'model':self.model,'state':state,'questions':questions})
        r.raise_for_status()
        j=r.json()
        out={}
        for k,a in j.get('answers',{}).items():
            t=a.get('type')
            if t=='choice':x={'answer':a['choice'],'confidence':a['confidence'],'probabilities':a['probabilities']}
            elif t=='noul':x={'answer':a['noul']>=0.5,'p':a['noul'],'confidence':abs(2*a['noul']-1),'probabilities':{'true':a['noul'],'false':1-a['noul']}}
            elif t=='score':x={'answer':int(max(a['probabilities'],key=a['probabilities'].get)),'score':a['score'],'confidence':a['confidence'],'probabilities':a['probabilities']}
            else:x={'answer':None,'confidence':0.0,'refused':True}
            x.update(model=j.get('model',self.model),accepted=x['confidence']>=self.threshold)
            out[k]=x
        for k in questions:out.setdefault(k,{'answer':None,'confidence':0.0,'missing':True,'model':self.model,'accepted':False})
        return out
class Llm:
    def __init__(self,transport=None):
        import httpx
        self.url,self.model=env('LLM_URL','https://api.anthropic.com/v1/messages'),env('LLM_MODEL','claude-sonnet-5-5')
        self.client=httpx.Client(timeout=600,headers={'x-api-key':env('ANTHROPIC_API_KEY',''),'anthropic-version':env('ANTHROPIC_VERSION','2026-10-21'),'content-type':'application/json'},transport=transport)
    def ask(self,system,text,max_tokens=8000):
        r=self.client.post(self.url,json={'model':self.model,'max_tokens':max_tokens,'system':system,'messages':[{'role':'user','content':text}]})
        r.raise_for_status()
        return ''.join(b.get('text','') for b in r.json().get('content',[]) if b.get('type')=='text').strip()
    def data(self,system,text,max_tokens=8000):
        for i in range(2):
            s=self.ask(system+' Reply with a single JSON object and nothing else.',text,max_tokens)
            m=re.search(r'\{.*\}',s,re.S)
            try:return json.loads(m.group(0))
            except (AttributeError,ValueError):text+='\n\nYour previous reply was not valid JSON. Reply with the JSON object only.'
        raise ValueError('model did not return JSON')
class Pdf:
    QUOTES=str.maketrans({'‘':"'",'’':"'",'“':'"','”':'"','–':'-','—':'-',' ':' '})
    @staticmethod
    def text(path):
        import pdfplumber
        with pdfplumber.open(path) as p:
            t='\n'.join(pg.extract_text() or '' for pg in p.pages)
            if len(t.strip())<50 and env('OCR','0')=='1':
                import pytesseract
                t='\n'.join(pytesseract.image_to_string(pg.to_image(resolution=300).original) for pg in p.pages)
        return t
    @staticmethod
    def norm(s):return re.sub(r'\s+',' ',(s or '').translate(Pdf.QUOTES)).strip().lower()
    @staticmethod
    def verbatim(span,text):return bool(Pdf.norm(span)) and Pdf.norm(span) in Pdf.norm(text)
    @staticmethod
    def ends(t,n=50000):return t if len(t)<=2*n else t[:n]+'\n[...]\n'+t[-n:]
class Lookup:
    def __init__(self,source,key,cols,many=False,keep=None,fields=None):
        files=sorted(glob.glob(source)) if source else []
        if not files:raise FileNotFoundError(source)
        self.many,cols=many,[c.lower() for c in cols]
        name=(key if isinstance(key,str) else 'k').lower()+hashlib.md5('|'.join(cols).encode()).hexdigest()[:6]
        self.path=files[0]+'.'+re.sub(r'\W','',name)+'.sqlite'
        if not os.path.exists(self.path):
            tmp=self.path+'.tmp'
            if os.path.exists(tmp):os.remove(tmp)
            db=sqlite3.connect(tmp)
            db.execute('create table kv(k text,v text)')
            for fn in files:
                with open(fn,newline='',encoding='utf-8-sig') as f:
                    b=[]
                    for r in csv.DictReader(f,fieldnames=fields):
                        r={str(a).strip().lower():v for a,v in r.items() if a is not None}
                        if keep and not keep(r):continue
                        k=key(r) if callable(key) else r.get(key.lower())
                        if k in (None,''):continue
                        b.append((str(k).strip().upper(),json.dumps({c:r.get(c) for c in cols})))
                        if len(b)>=50000:
                            db.executemany('insert into kv values(?,?)',b)
                            b=[]
                    db.executemany('insert into kv values(?,?)',b)
            db.execute('create index ix on kv(k)')
            db.commit()
            db.close()
            os.replace(tmp,self.path)
        self.db=sqlite3.connect(self.path)
    def get(self,k):
        r=[json.loads(v) for (v,) in self.db.execute('select v from kv where k=?',(str(k).strip().upper(),))]
        return r if self.many else (r[0] if r else None)
class Geo:
    NATIONAL=('conservation-area','listed-building','article-4-direction-area','flood-risk-zone','green-belt')
    GLA={'mol':211,'greenbelt-gla':210,'allocation':102,'conservation-gla':205,'listed-gla':214,'sil':206}
    def __init__(self):
        self.cache,self.uprns,self.ptals={},None,None
    def point(self,uprn):
        if not uprn:return None
        if self.uprns is None:self.uprns=Lookup(env('OS_OPEN_UPRN'),'uprn',('x_coordinate','y_coordinate'))
        r=self.uprns.get(uprn)
        return (float(r['x_coordinate']),float(r['y_coordinate'])) if r else None
    def layer(self,name):
        if name not in self.cache:
            p=env('LAYER_'+name.upper())
            if not p or not os.path.exists(p):self.cache[name]=None
            else:
                import geopandas
                g=geopandas.read_file(p)
                self.cache[name]=(g if g.crs else g.set_crs(4326)).to_crs(27700)
        return self.cache[name]
    def hits(self,name,x,y,buffer=0.0):
        g=self.layer(name)
        if g is None:return None
        from shapely.geometry import Point
        p=Point(x,y).buffer(buffer) if buffer else Point(x,y)
        return g.iloc[g.sindex.query(p,predicate='intersects')]
    def ptal(self,x,y):
        if not env('PTAL_GRID'):return None
        f=env('PTAL_FIELD','ptal2015').lower()
        if self.ptals is None:
            cx,cy=env('PTAL_X','x').lower(),env('PTAL_Y','y').lower()
            self.ptals=Lookup(env('PTAL_GRID'),lambda r:f'{int(float(r[cx])//100)}_{int(float(r[cy])//100)}',(f,))
        r=self.ptals.get(f'{int(x//100)}_{int(y//100)}')
        return r[f] if r else None
    def download(self,folder='data'):
        os.makedirs(folder,exist_ok=True)
        w,out=Web(os.path.join(folder,'raw')),{}
        for n in self.NATIONAL:
            p=os.path.join(folder,n+'.geojson')
            with open(p,'wb') as f:f.write(w.get(env('PLANNING_DATA_URL','https://files.planning.data.gov.uk/dataset/{name}.geojson').format(name=n)).content)
            out[n]=p
        base=env('GLA_MAP_URL','https://gis.london.gov.uk/arcgis/rest/services/apps/planning_data_map_02/MapServer')
        for n,i in self.GLA.items():
            feats,off=[],0
            while True:
                fs=w.get(f'{base}/{i}/query',params={'where':'1=1','outFields':'*','f':'geojson','outSR':'4326','resultOffset':off,'resultRecordCount':1000}).json().get('features',[])
                feats+=fs
                if len(fs)<1000:break
                off+=1000
            p=os.path.join(folder,f'gla-{n}.geojson')
            with open(p,'w') as f:json.dump({'type':'FeatureCollection','features':feats},f)
            out[n]=p
        return out
class Idox:
    def __init__(self,host,web,decision=''):
        self.base,self.web,self.decision=f'https://{host}/online-applications',web,decision
    def page(self,r):
        from selectolax.lexbor import LexborHTMLParser as HTMLParser
        return HTMLParser(r.text)
    def clean(self,s):return re.sub(r'\s+',' ',s or '').strip()
    def link(self,href):return urllib.parse.urljoin(self.base+'/',href or '')
    def tab(self,url,tab):return re.sub(r'activeTab=\w+','activeTab='+tab,url) if 'activeTab=' in url else url+('&' if '?' in url else '?')+'activeTab='+tab
    def table(self,r):
        out={}
        for tr in self.page(r).css('tr'):
            th,td=tr.css_first('th'),tr.css_first('td')
            if th and td:out[self.clean(th.text()).rstrip(':').lower()]=self.clean(td.text())
        return out
    def labels(self,url):return self.table(self.web.get(url))
    def results(self,r):
        t,rows=self.page(r),[]
        for li in t.css('li.searchresult'):
            a,m,ad=li.css_first('a'),li.css_first('p.metaInfo'),li.css_first('p.address')
            ref=re.search(r'Ref\. No:\s*([^|]+)',m.text() if m else '')
            if a and ref:rows.append({'reference':self.clean(ref.group(1)),'url':self.link(a.attributes.get('href')),'description':self.clean(a.text()),'address':self.clean(ad.text()) if ad else ''})
        nx=t.css_first('a.next')
        return rows,self.link(nx.attributes.get('href')) if nx else None
    def all(self,r):
        rows,nx=self.results(r)
        while nx:
            more,nx=self.results(self.web.get(nx))
            rows+=more
        return rows
    def form(self,action):
        c=self.page(self.web.get(f'{self.base}/search.do?action={action}&searchType=Application')).css_first('input[name=_csrf]')
        return {'_csrf':c.attributes.get('value')} if c else {}
    def search(self,start,end):
        d={**self.form('advanced'),'searchType':'Application','date(applicationDecisionStart)':start.strftime('%d/%m/%Y'),'date(applicationDecisionEnd)':end.strftime('%d/%m/%Y'),'caseDecision':self.decision}
        return self.all(self.web.post(f'{self.base}/advancedSearchResults.do?action=firstPage',data=d))
    def simple(self,text):
        r=self.web.post(f'{self.base}/simpleSearchResults.do?action=firstPage',data={**self.form('simple'),'searchType':'Application','searchCriteria.simpleSearchString':text,'searchCriteria.simpleSearch':'true'})
        if 'applicationDetails.do' in str(r.url):
            s=self.table(r)
            return [{'reference':s.get('reference',''),'url':str(r.url),'description':s.get('proposal',''),'address':s.get('address','')}]
        return self.all(r)
    def find(self,ref):
        k=re.sub(r'\s+','',ref.upper())
        return next((x['url'] for x in self.simple(ref) if re.sub(r'\s+','',x['reference'].upper())==k),None)
    def documents(self,url):
        out=[]
        for tr in self.page(self.web.get(self.tab(url,'documents'))).css('tr'):
            a=next((a for a in tr.css('a') if '/files/' in (a.attributes.get('href') or '') or '.pdf' in (a.attributes.get('href') or '').lower()),None)
            if a:out.append({'label':self.clean(' '.join(td.text() for td in tr.css('td'))),'url':self.link(a.attributes['href'])})
        return out
    def related(self,url):
        t=self.page(self.web.get(self.tab(url,'relatedCases')))
        apps,seen=[],set()
        for a in t.css('a'):
            h=a.attributes.get('href') or ''
            if 'applicationDetails.do' not in h:continue
            txt=self.clean(a.parent.text()) if a.parent else self.clean(a.text())
            m=re.search(r'Ref\. No:\s*([^|]+)',txt) or re.search(r'\b(\d{2,4}/\d{3,6}/[A-Z0-9/]+)\b',txt)
            if m and self.clean(m.group(1)) not in seen:
                seen.add(self.clean(m.group(1)))
                apps.append({'reference':self.clean(m.group(1)),'url':self.link(h),'description':self.clean(a.text())})
        p=next((self.link(a.attributes['href']) for a in t.css('a') if 'propertyDetails.do' in (a.attributes.get('href') or '')),None)
        return {'applications':apps,'property':p}
class Notify:
    def __init__(self,transport=None):
        import httpx
        k=env('NOTIFY_API_KEY','')
        self.service,self.secret=k[-73:-37],k[-36:]
        self.url=env('NOTIFY_URL','https://api.notifications.service.gov.uk')
        self.client=httpx.Client(timeout=30,transport=transport)
    def token(self):
        b=lambda d:base64.urlsafe_b64encode(json.dumps(d,separators=(',',':')).encode()).rstrip(b'=')
        h=b({'typ':'JWT','alg':'HS256'})+b'.'+b({'iss':self.service,'iat':int(time.time())})
        return (h+b'.'+base64.urlsafe_b64encode(hmac.new(self.secret.encode(),h,hashlib.sha256).digest()).rstrip(b'=')).decode()
    def send(self,kind,body):
        r=self.client.post(f'{self.url}/v2/notifications/{kind}',json=body,headers={'Authorization':'Bearer '+self.token()})
        r.raise_for_status()
        return r.json()
class Register(Tools):
    BOROUGHS={'croydon':('publicaccess3.croydon.gov.uk','idox'),'bromley':('planningaccess.bromley.gov.uk',None),'harrow':('planningsearch.harrow.gov.uk',None),'lambeth':('planning.lambeth.gov.uk','idox')}
    EXCLUDE={'X1':'householder','X2':'lawfulness','X3':'conditions','X4':'hmo','X5':'nonetgain','X6':'tenplus','X7':'notselfcontained','X8':'consultation','X9':'invalid','X10':'nondetermination'}
    FIELDS=('case_id','borough','week','reference','url','description','address','postcode','uprn','earlier_refs','resubmission_of','app_type','scheme_type','homes_existing','homes_proposed','homes_net','bedrooms','family_lost','decision_date','register_date','decided_by','recommendation','overturn','split','n_reasons','reasons','policies','s106_only','agent_named','certificate','conservation','listed','article4','flood_zone','greenbelt_mol','allocation','ptal','doc_notice','doc_form','doc_report','doc_drawings','deadline','days_left','critical','code','files','machine','verbatim','notice_decision','error','screened_by','registered_by','registered_on','checked','qc','stage')
    CONTACTS=('case_id','agent_name','agent_company','agent_address','agent_email','agent_phone','applicant_name','applicant_address','owner_name','owner_address')
    CRITICAL=('reference','decision_date','homes_net','code')
    TABLES={None:'register','X4':'hmo','X10':'nondetermination','review':'review'}
    def __init__(self,borough,week,store,vault,today=None):
        self.borough,self.store=borough,store
        self.host,self.platform=self.BOROUGHS[borough]
        self.week=str(week)
        self.today=today or date.today()
        os.makedirs(store,exist_ok=True)
        os.makedirs(vault,exist_ok=True)
        self.db=sqlite3.connect(os.path.join(store,'register.db'))
        self.vault=sqlite3.connect(os.path.join(vault,'contacts.db'))
        with self.db:
            for t in ('register','hmo','nondetermination','excluded','review'):self.db.execute(f'create table if not exists {t}(id text primary key,data text)')
            self.db.execute('create table if not exists log(at text,borough text,query text,refs text)')
        with self.vault:self.vault.execute('create table if not exists contacts(id text primary key,data text)')
    DOCS={'notice':r'decision notice|notice of decision|refusal notice|decision letter|\bdecision\b','form':r'application form','report':r"officer'?s? report|delegated report|committee report|report of handling|case officer report",'drawings':r'\b(?:plans?|drawings?|elevations?|sections?|layouts?)\b'}
    APPS={'full':'Full planning permission','outline':'Outline planning permission','householder':'Householder application for works to an existing house','prior':'Prior approval under permitted development','pip':'Permission in Principle','lawfulness':'Lawful development certificate','conditions':'Discharge or variation of conditions, or a non-material amendment','listed':'Listed building consent only','consultation':'Consultation from another council, or an EIA screening or scoping request','other':'None of these'}
    SCHEMES={'new build':'New homes built from scratch','conversion':'An existing building split or converted into more homes','change of use':'Change of use from another use to homes','prior approval':'Homes through prior approval under permitted development','mixed':'Homes together with other uses'}
    def idox(self):
        if self.platform!='idox':raise NotImplementedError(f'{self.borough}: register platform not confirmed')
        return self.tool('_idox',lambda:Idox(self.host,Web(os.path.join(self.store,'raw')),env('IDOX_DECISION_'+self.borough.upper(),'')))
    def search(self,start,end):
        ix,out=self.idox(),[]
        for r in ix.search(start,end):
            d=ix.labels(r['url']).get('decision','')
            if 'refus' in d.lower():out.append({**r,'decision':d})
        self.log(f'idox advanced search decided {start.isoformat()}..{end.isoformat()} decision={ix.decision or "any"}',[r['reference'] for r in out])
        return out
    def log(self,query,refs):
        with self.db:self.db.execute('insert into log values(?,?,?,?)',(datetime.now().isoformat(timespec='seconds'),self.borough,query,json.dumps(refs)))
    def norm(self,ref):return self.borough+':'+re.sub(r'\s+','',ref.upper())
    def key(self,ref):return re.sub(r'[/_-]*INVALID$','',self.norm(ref))
    def rows(self,table,week=None):
        out=[json.loads(d) for (d,) in self.db.execute(f'select data from {table} where id like ? order by id',(self.borough+':%',))]
        return [r for r in out if week is None or r.get('week')==str(week)]
    def ids(self,table,week=None):return [r['case_id'] for r in self.rows(table,week)]
    def dedupe(self,rows):
        kept,dropped={},[]
        for r in rows:
            n,k=self.norm(r['reference']),self.key(r['reference'])
            if n!=k:dropped.append({**r,'case_id':n,'code':'X9'})
            elif k not in kept:kept[k]={**r,'case_id':k}
        return list(kept.values()),dropped
    def units(self,d):
        if not isinstance(d,dict):return None
        n=[int(v) for v in d.values() if str(v).strip().isdigit()]
        return sum(n) if n else 0
    def form(self,row):
        fs=(row.get('files') or {}).get('form') or []
        if not fs:return {}
        t=Pdf.text(fs[0])
        d=self.llm().data('You read UK planning application forms and copy facts exactly as written. Never guess: use null when the form does not say.','Return JSON with keys: application_type, existing, proposed, certificate, agent_name, agent_company, agent_address, agent_email, agent_phone, applicant_name, applicant_address, owner_name, owner_address. existing and proposed map number of bedrooms ("1","2","3","4+") to the number of homes, summed across all tenures, from the residential units section; use {} if that section shows none and null if the form has no such section. certificate is the ownership certificate letter: A, B, C or D.\n\nFORM:\n'+t[:150000])
        ex,pr=self.units(d.get('existing')),self.units(d.get('proposed'))
        big=lambda x:sum(int(v) for k,v in (x or {}).items() if str(v).strip().isdigit() and int(re.sub(r'\D','',str(k)) or 0)>=3)
        q=self.jev().ask(str(row.get('description',''))+'\n'+str(d.get('application_type') or '')+'\n'+t[:20000],{'app_type':Jev.choice('What type of application is this?',self.APPS),'scheme_type':Jev.choice('What kind of housing scheme is this?',self.SCHEMES)})
        c=str(d.get('certificate') or '').strip().upper()[:1]
        out={'app_type':q['app_type']['answer'] if q['app_type']['accepted'] else None,'scheme_type':q['scheme_type']['answer'] if q['scheme_type']['accepted'] else None,'homes_existing':ex,'homes_proposed':pr,'homes_net':pr-ex if ex is not None and pr is not None else None,'bedrooms':d.get('proposed'),'family_lost':big(d.get('existing'))>big(d.get('proposed')) if ex is not None and pr is not None else None,'certificate':c if c in ('A','B','C','D') else None,'machine':{'form':q}}
        out.update({k:d[k] for k in self.CONTACTS if k!='case_id' and d.get(k)})
        return out
    def screen(self,row,form):
        a,n=form.get('app_type'),form.get('homes_net')
        if 'non-determin' in str(row.get('decision','')).lower():return 'X10'
        x={'householder':'X1','lawfulness':'X2','conditions':'X3','consultation':'X8'}.get(a)
        if x:return x
        if n is not None and n<=0:return 'X5'
        if n is not None and n>=10:return 'X6'
        q=self.jev().ask(str(row.get('description',''))+'\n'+str(row.get('address','')),{'homes':Jev.choice('What homes does this proposal create?',{'self-contained':'Self-contained homes: houses or flats each with its own kitchen and bathroom','hmo':'A house in multiple occupation (HMO)','shared':'Rooms that are not self-contained: co-living, student rooms, a hostel or bedsits','none':'No new homes'})})['homes']
        form.setdefault('machine',{})['screen']=q
        if not q['accepted'] or a in (None,'listed','other') and n is None:return 'review'
        return {'hmo':'X4','shared':'X7','none':'X5'}.get(q['answer'],None if n is not None else 'review')
    def notice(self,row):
        fs=(row.get('files') or {}).get('notice') or []
        if not fs:return {'verbatim':False}
        t=Pdf.text(fs[0])
        d=self.llm().data('You copy text from UK planning decision notices exactly, character for character. Never paraphrase, shorten or summarise.','Return JSON with keys: decision_date (the date the notice was issued, as printed), reasons (array of strings: each reason for refusal copied word for word, without its number), policies (array of strings: each development plan policy reference copied exactly as printed).\n\nNOTICE:\n'+t[:150000])
        rs=[x.strip() for x in d.get('reasons') or [] if isinstance(x,str) and x.strip()]
        ps=[x.strip() for x in d.get('policies') or [] if isinstance(x,str) and x.strip()]
        q=self.jev().ask(t,{'decision':Jev.choice('What did the council decide?',{'refused':'Refused in full','split':'Part granted and part refused','granted':'Granted or approved','other':'Anything else'}),'decided_by':Jev.choice('Who made the decision?',{'delegated':'Officers under delegated powers','committee':'A planning committee'}),'s106_only':Jev.noul('Is the only reason for refusal the lack of a completed legal agreement or planning obligation?','Every reason concerns the missing legal agreement or obligation','At least one reason is about something else')})
        ok=lambda k:q[k]['answer'] if q[k]['accepted'] else None
        return {'decision_date':when(d.get('decision_date')),'reasons':rs,'policies':ps,'n_reasons':len(rs),'verbatim':bool(rs) and all(Pdf.verbatim(x,t) for x in rs+ps),'notice_decision':ok('decision'),'decided_by':ok('decided_by'),'s106_only':ok('s106_only'),'checked':self.today.isoformat(),'machine':{'notice':q}}
    def report(self,row):
        fs=(row.get('files') or {}).get('report') or []
        if not fs:return {'recommendation':None}
        q=self.jev().ask(Pdf.ends(Pdf.text(fs[0])),{'recommendation':Jev.choice('What did the case officer recommend?',{'approve':'Grant or approve','refuse':'Refuse','none':'No recommendation stated'})})
        a=q['recommendation']
        return {'recommendation':a['answer'] if a['accepted'] and a['answer']!='none' else None,'machine':{'report':q}}
    def history(self,row):
        ix=self.idox()
        rel=ix.related(row['url'])
        me=self.key(row['reference'])
        apps=[a for a in rel['applications'] if self.key(a['reference'])!=me]
        uprn=ix.labels(rel['property']).get('uprn') if rel['property'] else None
        prior=None
        for a in apps[:int(env('HISTORY_LIMIT','10'))]:
            s=ix.labels(a['url'])
            dd=when(s.get('decision issued date') or s.get('decision date'))
            if 'refus' in s.get('decision','').lower() and dd and row.get('decision_date') and dd<row['decision_date'] and (prior is None or dd>prior[1]):prior=(a['reference'],dd)
        return {'uprn':uprn or row.get('uprn'),'earlier_refs':[a['reference'] for a in apps],'resubmission_of':prior[0] if prior else None}
    def context(self,row):
        g=self.tool('_geo',Geo)
        p=g.point(row.get('uprn'))
        if not p:return {}
        x,y=p
        has=lambda n,b=0.0:None if (h:=g.hits(n,x,y,b)) is None else len(h)>0
        a4,fz=g.hits('article4',x,y),g.hits('flood',x,y)
        nm,lv=env('ARTICLE4_NAME','name'),env('FLOOD_LEVEL','flood-risk-level')
        gb,mo=has('greenbelt'),has('mol')
        return {'conservation':has('conservation'),'listed':has('listed',float(env('LISTED_BUFFER','10'))),'article4':None if a4 is None else sorted({str(v) for v in a4[nm]}) if nm in a4.columns else len(a4),'flood_zone':None if fz is None else max([int(float(v)) for v in fz[lv] if str(v).replace('.','',1).isdigit()],default=1) if lv in fz.columns else (2 if len(fz) else 1),'greenbelt_mol':None if gb is None and mo is None else bool(gb or mo),'allocation':has('allocation'),'ptal':g.ptal(x,y)}
    def flags(self,notice,report):
        db,rc,nd=notice.get('decided_by'),report.get('recommendation'),notice.get('notice_decision')
        return {'split':None if nd is None else nd=='split','overturn':None if db is None or db=='committee' and rc is None else db=='committee' and rc=='approve','s106_only':notice.get('s106_only')}
    def documents(self,row):
        ix,files=self.idox(),{k:[] for k in self.DOCS}
        for d in ix.documents(row['url']):
            k=next((k for k,p in self.DOCS.items() if re.search(p,d['label'],re.I)),None)
            if k and len(files[k])<int(env('MAX_'+k.upper(),'20' if k=='drawings' else '1')):files[k].append(ix.web.file(d['url']))
        return {'files':files,**{'doc_'+k:'saved' if v else 'missing' for k,v in files.items()}}
    def clock(self,d):
        d=date.fromisoformat(d) if isinstance(d,str) else d
        y,m=d.year+(d.month+5)//12,(d.month+5)%12+1
        dl=date(y,m,min(d.day,calendar.monthrange(y,m)[1]))
        left=(dl-self.today).days
        return {'deadline':dl.isoformat(),'days_left':left,'critical':0<=left<=70}
    def record(self,row):
        row={**row,**self.documents(row)}
        f=self.form(row)
        m=dict(f.pop('machine',{}))
        code=self.screen(row,{**f,'machine':m})
        base={**row,**f,'machine':m,'screened_by':'machine'}
        if code:return {**{k:v for k,v in base.items() if k not in self.CONTACTS or k=='case_id'},'code':code}
        n=self.notice(row)
        m.update(n.pop('machine',{}))
        r=self.report(row)
        m.update(r.pop('machine',{}))
        h=self.history({**row,**n})
        rec={**base,**n,**r,**h,**self.context({**row,**h}),**self.flags(n,r),'code':None,'registered_by':'machine','registered_on':self.today.isoformat()}
        if n.get('decision_date'):rec.update(self.clock(n['decision_date']))
        if n.get('notice_decision') not in ('refused','split'):rec['code']='review'
        rec['stage']='documents' if rec.get('doc_notice')=='saved' and rec.get('doc_form')=='saved' and n.get('verbatim') and n.get('decision_date') and rec['code'] is None else 'registered'
        return rec
    def scrub(self,record):
        clean={k:record[k] for k in self.FIELDS if k in record}
        if any(k in record for k in ('agent_name','agent_company')):clean['agent_named']=bool(record.get('agent_name') or record.get('agent_company'))
        return clean,{k:record[k] for k in self.CONTACTS if k in record}
    def save(self,record):
        record,contacts=self.scrub(record)
        t=self.TABLES.get(record.get('code'),'excluded')
        with self.db:
            old=self.db.execute(f'select data from {t} where id=?',(record['case_id'],)).fetchone()
            record={'borough':self.borough,'week':self.week,**(json.loads(old[0]) if old else {}),**record}
            self.db.execute(f'insert or replace into {t} values(?,?)',(record['case_id'],json.dumps(record,default=str)))
        if len(contacts)>1:
            with self.vault:self.vault.execute('insert or replace into contacts values(?,?)',(contacts['case_id'],json.dumps(contacts,default=str)))
        return record
    def purge(self,ids):
        with self.vault:self.vault.executemany('delete from contacts where id=?',[(i,) for i in ids])
    def reconcile(self):
        found={self.norm(r) for (refs,) in self.db.execute('select refs from log where borough=?',(self.borough,)) for r in json.loads(refs)}
        return sorted(found-{i for t in ('register','hmo','nondetermination','excluded') for i in self.ids(t)})
    def datahub(self,start,end):
        w=self.tool('_hub',lambda:Web(os.path.join(self.store,'raw')))
        f={k:env('PLD_FIELD_'+k.upper(),v) for k,v in (('borough','lpa_name'),('ref','lpa_app_no'),('date','decision_date'),('decision','decision'))}
        q={'size':int(env('PLD_SIZE','10000')),'query':{'bool':{'filter':[{'range':{f['date']:{'gte':start.isoformat(),'lte':end.isoformat(),'format':'yyyy-MM-dd||dd/MM/yyyy'}}},{'match_phrase':{f['borough']:self.borough}}]}}}
        r=w.post(env('PLD_URL','https://planningdata.london.gov.uk/api-guest/applications/_search'),json=q,headers={'X-API-AllowRequest':env('PLD_GUEST_KEY','')})
        hub={self.norm(str(h['_source'].get(f['ref'],''))) for h in r.json().get('hits',{}).get('hits',[]) if 'refus' in str(h['_source'].get(f['decision'],'')).lower()}-{self.norm('')}
        found=set()
        for qy,refs in self.db.execute('select query,refs from log where borough=?',(self.borough,)):
            m=re.search(r'decided (\S+)\.\.(\S+)',qy)
            if m and start.isoformat()<=m.group(1) and m.group(2)<=end.isoformat():found|={self.norm(x) for x in json.loads(refs)}
        return {'datahub':len(hub),'register':len(found),'missed_by_register':sorted(hub-found),'missed_by_datahub':sorted(found-hub),'share_missed_by_datahub':len(found-hub)/len(found) if found else None}
    def audit(self,rate=0.1):
        rng=random.Random(self.borough+self.week)
        ex,ins=self.ids('excluded',self.week),self.ids('register',self.week)
        a,b=rng.sample(ex,math.ceil(len(ex)*rate)),rng.sample(ins,math.ceil(len(ins)*rate))
        for i in b:self.save({'case_id':i,'qc':'pending'})
        return a,b
    def score(self,first,second):
        both=[i for i in first if i in second]
        if not both:return {}
        miss={i:[f for f in self.CRITICAL if str(first[i].get(f))!=str(second[i].get(f))] for i in both}
        for i,m in miss.items():self.save({'case_id':i,'qc':'failed' if m else 'passed'})
        out={f:sum(f in m for m in miss.values())/len(both) for f in self.CRITICAL}
        out['any']=sum(bool(m) for m in miss.values())/len(both)
        return out
    def fresh(self):
        at=self.db.execute('select max(at) from log where borough=?',(self.borough,)).fetchone()[0]
        return at is not None and (self.today-date.fromisoformat(at[:10])).days<=7
    def ready(self):
        out=[]
        for r in self.rows('register'):
            if r.get('stage')=='documents' and r.get('qc') in (None,'passed'):r['stage']='ready'
            if r.get('stage')=='ready':
                r.update(self.clock(r['decision_date']))
                out.append(self.save(r))
        return sorted(out,key=lambda r:r['deadline'])
    def backfill(self,file,n,seed='phase1'):
        c={k:env('BACKFILL_'+k.upper(),k) for k in ('reference','borough','scheme_type','decision_date')}
        mp=dict(x.split('=',1) for x in env('BACKFILL_MAP','').split(',') if '=' in x)
        with open(file,newline='',encoding='utf-8-sig') as fh:rows=[r for r in csv.DictReader(fh) if (when(r.get(c['decision_date'])) or '')[:4] in ('2021','2022')]
        strata={}
        for r in rows:strata.setdefault((r.get(c['borough'],'').strip().lower(),r.get(c['scheme_type'],'').strip().lower()),[]).append(r)
        rng,pick=random.Random(seed),[]
        for k in sorted(strata):
            g=sorted(strata[k],key=lambda r:r[c['reference']])
            pick+=rng.sample(g,min(len(g),max(1,round(n*len(g)/len(rows)))))
        mine=[r for r in pick if r.get(c['borough'],'').strip().lower()==self.borough]
        out={'sample':len(pick),'borough':len(mine),'verified':0,'reports_online':0,'diffs':{},'failed':[]}
        for r in mine:
            ref=r[c['reference']].strip()
            try:
                url=self.idox().find(ref)
                if not url:raise ValueError('not found on the register')
                s=self.idox().labels(url)
                rec=self.save({**self.record({'case_id':self.key(ref),'reference':ref,'url':url,'description':s.get('proposal',''),'address':s.get('address',''),'decision':s.get('decision','')}),'week':'backfill'})
                out['verified']+=1
                out['reports_online']+=rec.get('doc_report')=='saved'
                out['diffs'][ref]=self.diff({mp.get(k,k):(when(v) or v) if mp.get(k,k).endswith('_date') else v for k,v in r.items() if mp.get(k,k) in self.FIELDS and mp.get(k,k)!='reference'},rec)
            except Exception as e:out['failed'].append((ref,str(e)[:200]))
        return out
    def diff(self,old,new):return {k:(v,new.get(k)) for k,v in old.items() if str(v)!=str(new.get(k))}
    def run(self):
        d=date.fromisoformat(self.week)
        out={'searched':0,'saved':{},'errors':[]}
        done={i for t in ('register','hmo','nondetermination','excluded','review') for i in self.ids(t)}
        for s,e in ((d-timedelta(days=7),d-timedelta(days=1)),(d-timedelta(days=21),d-timedelta(days=15))):
            kept,dropped=self.dedupe(self.search(s,e))
            out['searched']+=len(kept)+len(dropped)
            for x in dropped:self.save(x)
            for r in kept:
                if r['case_id'] in done:continue
                try:rec=self.record(r)
                except Exception as ex:
                    rec={**r,'code':'review','error':str(ex)[:500]}
                    out['errors'].append((r['case_id'],str(ex)[:200]))
                self.save(rec)
                done.add(r['case_id'])
                k=str(rec.get('code'))
                out['saved'][k]=out['saved'].get(k,0)+1
        out.update(ready=len(self.ready()),gaps=self.reconcile(),fresh=self.fresh())
        return out
class Coding(Tools):
    CATEGORIES=('E','D','J','P','U')
    RANK={'E':0,'D':1,'J':2,'U':3,'P':4}
    THEMES=('information','agreement','design','character','heritage','amenity','quality','family','employment','greenbelt','transport','flood','trees','other')
    def __init__(self,register,store):
        os.makedirs(store,exist_ok=True)
        self.src=sqlite3.connect(f'file:{os.path.abspath(register)}?mode=ro',uri=True)
        self.db=sqlite3.connect(os.path.join(store,'coding.db'))
        with self.db:
            for t in ('cases','reasons','second'):self.db.execute(f'create table if not exists {t}(id text primary key,cid text,data text)')
    def now(self):return datetime.now().isoformat(timespec='seconds')
    def get(self,table,key):
        r=self.db.execute(f'select data from {table} where id=?',(key,)).fetchone()
        return json.loads(r[0]) if r else None
    def put(self,table,key,row):self.db.execute(f'insert or replace into {table} values(?,?,?)',(key,row['case_id'],json.dumps(row,default=str)))
    def rows(self,table,cid=None):
        q=self.db.execute(f'select data from {table} order by id') if cid is None else self.db.execute(f'select data from {table} where cid=? order by id',(cid,))
        return [json.loads(d) for (d,) in q]
    def intake(self):
        have={c['case_id'] for c in self.rows('cases')}
        new=[c for c in (json.loads(d) for (d,) in self.src.execute('select data from register order by id')) if c.get('stage')=='ready' and c['case_id'] not in have]
        with self.db:
            for c in new:
                rs=self.split(c)
                self.put('cases',c['case_id'],{'case_id':c['case_id'],'facts':c,'source':'report' if c.get('doc_report')=='saved' else 'notice','status':'open' if rs else 'empty','taken':self.now()})
                for r in rs:self.put('reasons',r['reason_id'],r)
        return [c['case_id'] for c in new]
    def split(self,case):return [{'reason_id':f"{case['case_id']}#{n:02d}",'case_id':case['case_id'],'n':n,'text':t,'status':'open'} for n,t in enumerate(case.get('reasons') or [],1)]
    CUES={'E':'Evidence: a survey, study, detail or legal agreement was missing or inadequate','D':'Design defect: the scheme fails a measurable standard such as internal space, storage, amenity space, accessibility, or cycle and bin storage, fixable by redesign','J':'Judgement: harm to character, appearance, heritage, neighbours or amenity as a matter of planning judgement','P':'Principle: policy rules out this use or development on this site, such as Green Belt, loss of a family home or loss of employment land','U':'Unclear: the reason fits none of these'}
    TOPICS={'information':'Information missing: a survey, study or detail not submitted','agreement':'No signed legal agreement or planning obligation','design':'Design, scale, form, massing or appearance','character':'Harm to the character of the area or street scene','heritage':'Harm to a listed building, conservation area or other heritage asset','amenity':'Harm to neighbours: daylight, sunlight, outlook, privacy, overbearing effect or noise','quality':'Quality of the new homes: space, light, outlook, amenity space, storage or accessibility','family':'Loss of a family home or the wrong housing mix','employment':'Loss of employment, commercial or industrial space','greenbelt':'Green Belt or Metropolitan Open Land','transport':'Parking, highways, access, cycle storage or servicing','flood':'Flood risk or drainage','trees':'Trees, landscaping, ecology or biodiversity','other':'Anything else'}
    def policies(self,reason):
        t=reason['text'] if isinstance(reason,dict) else reason
        d=self.llm().data('You list the planning policies cited in a refusal reason, copying each reference exactly as written.','Return JSON {"policies":[{"policy":"the policy reference as written","plan":"the plan name as written, or null"}]}.\n\nREASON:\n'+t)
        return [p for p in d.get('policies') or [] if isinstance(p,dict) and p.get('policy') and Pdf.verbatim(p['policy'],t)]
    def cues(self,text):
        q=self.jev().ask(text,{'category':Jev.choice('Which type of refusal reason is this?',self.CUES),'theme':Jev.choice('What is the main topic of this refusal reason?',self.TOPICS)})
        return {k:{x:q[k].get(x) for x in ('answer','confidence','probabilities','model','accepted')} for k in q}
    def suggest(self,reason):
        r=self.get('reasons',reason) if isinstance(reason,str) else reason
        r['suggested']={**self.cues(r['text']),'policies':self.policies(r['text']),'on':self.now()}
        with self.db:self.put('reasons',r['reason_id'],r)
        return r['suggested']
    def check(self,theme,category):
        if theme not in self.THEMES or category not in self.CATEGORIES:raise ValueError((theme,category))
        if theme=='agreement' and category!='E':raise ValueError((theme,category))
    def code(self,reason_id,coder,theme,category,second=False):
        self.check(theme,category)
        r=self.get('reasons',reason_id)
        if r is None:raise KeyError(reason_id)
        other=self.get('reasons' if second else 'second',reason_id)
        if other and other.get('coder')==coder:raise ValueError('same coder')
        if second and not self.get('cases',r['case_id']).get('double'):raise ValueError('not sampled')
        if not second and r['status'] not in ('open','coded'):raise ValueError(r['status'])
        row={k:r[k] for k in ('reason_id','case_id','n','text')} if second else r
        row.update(theme=theme,category=category,coder=coder,coded_on=self.now(),status='coded')
        with self.db:self.put('second' if second else 'reasons',reason_id,row)
    def sample(self,rate=0.1,seed=None):
        seed=seed or self.now()[:10]
        pool=[c['case_id'] for c in self.rows('cases') if c['status']=='open' and 'double' not in c]
        pick=set(random.Random(seed).sample(pool,math.ceil(len(pool)*rate)))
        with self.db:
            for i in pool:
                c=self.get('cases',i)
                c.update(double=i in pick,seed=seed)
                self.put('cases',i,c)
        return sorted(pick)
    def agree(self):
        pairs=[(self.get('reasons',s['reason_id']),s) for s in self.rows('second')]
        pairs=[(f,s) for f,s in pairs if f.get('category')]
        if not pairs:return {}
        n=len(pairs)
        po=sum(f['category']==s['category'] for f,s in pairs)/n
        pe=sum(sum(f['category']==c for f,_ in pairs)*sum(s['category']==c for _,s in pairs) for c in self.CATEGORIES)/n/n
        with self.db:
            for f,s in pairs:
                if f['status']=='coded' and any(f[k]!=s[k] for k in ('theme','category')):
                    f['status']='disputed'
                    self.put('reasons',f['reason_id'],f)
        return {'n':n,'agree':po,'kappa':1.0 if pe==1 else (po-pe)/(1-pe),'theme':sum(f['theme']==s['theme'] for f,s in pairs)/n}
    def adjudicate(self,reason_id,coder,theme,category):
        self.check(theme,category)
        r=self.get('reasons',reason_id)
        if r['status']!='disputed':raise ValueError(r['status'])
        r.update(status='adjudicated',adjudicated={'theme':theme,'category':category,'by':coder,'on':self.now()})
        with self.db:self.put('reasons',reason_id,r)
    def case(self,case_id):
        c,rs=self.get('cases',case_id),self.rows('reasons',case_id)
        if not rs or 'double' not in c or any(r['status'] not in ('coded','adjudicated') for r in rs):return None
        if c['double'] and len(self.rows('second',case_id))<len(rs):return None
        cats=[(r.get('adjudicated') or r)['category'] for r in rs]
        return {'category':max(cats,key=self.RANK.get),'counts':{k:cats.count(k) for k in self.CATEGORIES}}
    def done(self):
        self.agree()
        out=[]
        with self.db:
            for c in self.rows('cases'):
                r=self.case(c['case_id']) if c['status']=='open' else None
                if r:
                    c.update(r,status='coded',coded_on=self.now())
                    self.put('cases',c['case_id'],c)
                    out.append(c)
        return sorted(out,key=lambda c:c['facts'].get('deadline') or '')
    def run(self,rate=0.1):
        new,picked=self.intake(),self.sample(rate)
        done,errors=0,[]
        for r in self.rows('reasons'):
            if 'suggested' in r:continue
            try:
                self.suggest(r)
                done+=1
            except Exception as e:errors.append((r['reason_id'],str(e)[:200]))
        return {'intake':new,'double':picked,'suggested':done,'errors':errors,'agreement':self.agree(),'released':[c['case_id'] for c in self.done()]}
class Panel(Tools):
    ROUTES=('stop','redesign','resubmit','appeal','resubmit_appeal','alternative','preapp')
    ROLES=('dm','consultant','design','heritage','daylight','highways')
    VIEWS=('sound','arguable','weak')
    BANDS=('very unlikely','unlikely','finely balanced','likely','very likely')
    MID=(0.1,0.3,0.5,0.7,0.9)
    BASE={'returns':(0.19,'cohort: refused small schemes back and approved within 36 months'),'appeals':(0.30,'Planning Inspectorate 2025/26: written representations appeals allowed')}
    TIMING={'resubmit':('4-8 weeks','median 84 days','full application fee plus the revised work'),'appeal':('2-4 weeks','median 20 weeks','no new evidence allowed; risk of a costs order if unreasonable'),'preapp':('2-3 months','then as for resubmission','pre-application fee plus redesign')}
    def __init__(self,coding,store,today=None,backfill=False):
        os.makedirs(store,exist_ok=True)
        self.today=today or date.today()
        self.backfill=backfill
        self.src=sqlite3.connect(f'file:{os.path.abspath(coding)}?mode=ro',uri=True)
        self.db=sqlite3.connect(os.path.join(store,'panel.db'))
        with self.db:
            for t in ('cases','ratings','members','declarations'):self.db.execute(f'create table if not exists {t}(id text primary key,cid text,data text)')
    def now(self):return datetime.now().isoformat(timespec='seconds')
    def get(self,table,key):
        r=self.db.execute(f'select data from {table} where id=?',(key,)).fetchone()
        return json.loads(r[0]) if r else None
    def put(self,table,key,row):self.db.execute(f'insert or replace into {table} values(?,?,?)',(key,row.get('case_id',''),json.dumps(row,default=str)))
    def rows(self,table,cid=None):
        q=self.db.execute(f'select data from {table} order by id') if cid is None else self.db.execute(f'select data from {table} where cid=? order by id',(cid,))
        return [json.loads(d) for (d,) in q]
    def member(self,name,role,boroughs):
        if role not in self.ROLES:raise ValueError(role)
        with self.db:self.put('members',name,{'name':name,'role':role,'boroughs':boroughs})
    def declare(self,name,case_id,why):
        if why not in ('interest','advised'):raise ValueError(why)
        with self.db:self.put('declarations',f'{case_id}|{name}',{'case_id':case_id,'member':name,'why':why,'on':self.now()})
    def recused(self,name,case_id):
        m,c=self.get('members',name),self.get('cases',case_id)
        if m is None:raise KeyError(name)
        last=m['boroughs'].get(c['facts'].get('borough'))
        return bool(last and (self.today-date.fromisoformat(last)).days<=731 or self.get('declarations',f'{case_id}|{name}'))
    def intake(self):
        have={c['case_id'] for c in self.rows('cases')}
        new=[c for c in (json.loads(d) for (d,) in self.src.execute('select data from cases order by id')) if c.get('status')=='coded' and c['case_id'] not in have]
        with self.db:
            for c in new:
                rs=[json.loads(d) for (d,) in self.src.execute('select data from reasons where cid=? order by id',(c['case_id'],))]
                reasons=[{'n':r['n'],'text':r['text'],'theme':(r.get('adjudicated') or r)['theme'],'category':(r.get('adjudicated') or r)['category']} for r in rs]
                self.put('cases',c['case_id'],{'case_id':c['case_id'],'facts':c['facts'],'source':c['source'],'category':c['category'],'counts':c['counts'],'reasons':reasons,'homes':c['facts'].get('homes_net'),'taken':self.now()})
        return [c['case_id'] for c in new]
    def days(self,c):return (date.fromisoformat(c['facts']['deadline'])-self.today).days
    def fast(self,c):return 0<=self.days(c)<=70
    def quorum(self,c):return 2 if self.backfill or self.fast(c) or c.get('reopened') else 3
    def card(self,case_id):
        c=self.get('cases',case_id)
        f=c['facts']
        return {'case_id':case_id,'days_left':self.days(c),'fast':self.fast(c),'homes':c['homes'],'source':c['source'],'reasons':c['reasons'],'overturn':f.get('overturn'),'recommendation':f.get('recommendation'),'context':{k:f.get(k) for k in ('conservation','listed','article4','flood_zone','greenbelt_mol','allocation','ptal')},'base':self.BASE,'crib':self.crib(f.get('borough')),'precedents':self.precedents(f.get('borough'),sorted({r['theme'] for r in c['reasons']}))}
    def crib(self,borough):
        d=env('CRIB_DIR','crib')
        p=os.path.join(d,f'{borough}.json')
        if os.path.exists(p):
            with open(p) as f:return json.load(f)
        src=env('LOCAL_PLAN_'+str(borough).upper())
        if not src or not os.path.exists(src):return None
        t=Pdf.text(src)
        x=self.llm().data('You summarise the local plan policies that apply to small housing schemes of 1 to 9 homes. Copy policy references exactly.','Return JSON {"policies":[{"policy":"reference as written","title":"title as written","test":"one sentence: what the policy requires of a small housing scheme"}]}.\n\nPLAN:\n'+t[:150000])
        ps=[q for q in x.get('policies') or [] if isinstance(q,dict) and q.get('policy') and Pdf.verbatim(q['policy'],t)]
        for q in ps:
            a=self.jev().ask(f"{q.get('title','')}: {q.get('test','')}",{'firm':Jev.choice('Is this policy a firm presumption or a matter of judgement?',{'firm':'A firm rule: a clear standard or prohibition that is either met or not','judgement':'A matter of judgement: weighs harm or quality and leaves discretion'})})['firm']
            q.update(firmness=a['answer'] if a['accepted'] else None,machine=a)
        c={'borough':borough,'status':'draft','policies':ps,'drafted':self.now()}
        os.makedirs(d,exist_ok=True)
        with open(p+'.draft','w') as f:json.dump(c,f,indent=1)
        return c
    def precedents(self,borough,themes):
        items={}
        t=env('TRACKING_DB')
        if t and os.path.exists(t):
            db=sqlite3.connect(f'file:{os.path.abspath(t)}?mode=ro',uri=True)
            bs={}
            for (d,) in db.execute('select data from cases'):
                c=json.loads(d)
                bs[c['case_id']]=c['borough']
            for (d,) in db.execute('select data from events'):
                e=json.loads(d)
                if e['kind']=='appeal_decision' and e['status']=='confirmed' and e.get('issues') and bs.get(e['case_id'])==borough:items[e['ref']]=e['issues']['items']
        s=env('SUPPORT_DB')
        if s and os.path.exists(s):
            db=sqlite3.connect(f'file:{os.path.abspath(s)}?mode=ro',uri=True)
            for (d,) in db.execute('select data from precedents'):
                e=json.loads(d)
                if e['borough']==borough:items.setdefault(e['reference'],e['issues'])
        out={}
        for its in items.values():
            for i in its:
                if i['theme'] in themes:
                    g=out.setdefault(i['theme'],[0,0])
                    g[0]+=1
                    g[1]+=i['upheld']
        return {k:{'n':n,'upheld':u/n} for k,(n,u) in sorted(out.items())}
    def agenda(self,limit=16):
        todo=sorted((c for c in self.rows('cases') if not c.get('decision')),key=lambda c:(c['facts']['deadline'],-(c.get('homes') or 0)))
        return [c['case_id'] for c in todo if not self.fast(c)][:limit],[c['case_id'] for c in todo if self.fast(c)]
    def rate(self,name,case_id,route,band,alt=None,altband=None):
        c=self.get('cases',case_id)
        if c.get('revealed'):raise ValueError('revealed')
        if self.recused(name,case_id):raise ValueError('recused')
        if route not in self.ROUTES or band not in self.BANDS or alt not in self.ROUTES+(None,) or altband not in self.BANDS+(None,) or (alt is None)!=(altband is None):raise ValueError((route,band,alt,altband))
        with self.db:self.put('ratings',f'{case_id}|{name}',{'case_id':case_id,'member':name,'route':route,'band':band,'alt':alt,'altband':altband,'on':self.now()})
    def reveal(self,case_id):
        c=self.get('cases',case_id)
        rs=[r for r in self.rows('ratings',case_id) if not self.recused(r['member'],case_id)]
        if len(rs)<self.quorum(c):raise ValueError('quorum')
        c['revealed']=self.now()
        with self.db:self.put('cases',case_id,c)
        return rs
    def differ(self,case_id):
        rs=self.rows('ratings',case_id)
        if len(rs)<2:return None
        b=[self.BANDS.index(r['band']) for r in rs]
        return len({r['route'] for r in rs})>1 or max(b)-min(b)>1
    def refer(self,case_id,specialist,question):
        c=self.get('cases',case_id)
        c.setdefault('referrals',[]).append({'specialist':specialist,'question':question,'asked':self.today.isoformat(),'due':(self.today+timedelta(days=7)).isoformat(),'view':None})
        with self.db:self.put('cases',case_id,c)
    def answer(self,case_id,specialist,view):
        c=self.get('cases',case_id)
        for r in c.get('referrals',[]):
            if r['specialist']==specialist and r['view'] is None:r['view']=view
        with self.db:self.put('cases',case_id,c)
    def propose(self,case_id,views):
        c=self.get('cases',case_id)
        v={int(n):(x[0] if isinstance(x,(list,tuple)) else x) for n,x in views.items()}
        rs=[(r,v[r['n']]) for r in c['reasons']]
        has=lambda k,w:any(r['category']==k and x==w for r,x in rs)
        if has('P','sound'):return {'route':'stop','why':'a principle reason is sound'}
        if has('P','arguable'):return {'route':'redesign','why':'a principle reason is only arguable: redesign to meet the policy exception'}
        if any(r['category']=='E' and r['theme']!='agreement' and x!='weak' for r,x in rs):return {'route':'resubmit_appeal' if has('J','weak') else 'resubmit','why':'a material evidence gap cannot be cured at appeal'}
        if all(x=='weak' or r['theme']=='agreement' for r,x in rs) and self.days(c)>=0:return {'route':'appeal','why':'every reason is weak'+(', and councillors refused against officer advice' if c['facts'].get('overturn') else ''),'costs':bool(c['facts'].get('overturn'))}
        if has('J','sound'):return {'route':'preapp','why':'a judgement reason is sound: a materially different scheme needs pre-application advice'}
        return {'route':'resubmit','why':'design defects or arguable judgement reasons that a revised scheme can answer'}
    def decide(self,case_id,members,views,route,band,alt,altband,homes,changes,interest,rationale,dissent=None):
        c=self.get('cases',case_id)
        rated={r['member'] for r in self.rows('ratings',case_id)}
        ns=[r['n'] for r in c['reasons']]
        if not c.get('revealed'):raise ValueError('not revealed')
        if any(r['view'] is None for r in c.get('referrals',[])):raise ValueError('referral open')
        if len(set(members))<self.quorum(c) or any(m not in rated or self.recused(m,case_id) for m in members):raise ValueError('members')
        if route not in self.ROUTES or band not in self.BANDS or alt not in self.ROUTES+(None,) or altband not in self.BANDS+(None,) or (alt is None)!=(altband is None):raise ValueError((route,band,alt,altband))
        if sorted(views)!=ns or any(v not in self.VIEWS or not l or '\n' in l for v,l in views.values()):raise ValueError('views')
        if not {'good','gains','losses'}<=set(interest):raise ValueError('interest')
        if not 150<=len(rationale.split())<=250:raise ValueError('rationale')
        if route!='stop' and any(r['category']=='P' and views[r['n']][0]=='sound' for r in c['reasons']):raise ValueError('sound principle reason: stop')
        if route=='appeal' and (self.days(c)<0 or any(views[r['n']][0]!='weak' and r['theme']!='agreement' for r in c['reasons'])):raise ValueError('appeal needs every reason weak and the deadline open')
        if not interest['good'] and route in ('appeal','resubmit_appeal'):raise ValueError('poor homes')
        if c.get('decision'):c.setdefault('history',[]).append(c['decision'])
        c['decision']={'members':sorted(set(members)),'views':[{'n':n,'view':views[n][0],'line':views[n][1]} for n in ns],'route':route,'band':band,'alt':alt,'altband':altband,'homes':homes,'changes':changes,'interest':interest,'rationale':rationale,'dissent':dissent,'costs':route=='appeal' and bool(c['facts'].get('overturn')),'obligation':route=='appeal' and any(r['theme']=='agreement' for r in c['reasons']),'on':self.now()}
        with self.db:self.put('cases',case_id,c)
        return c['decision']
    def reopen(self,case_id,why):
        c=self.get('cases',case_id)
        c.setdefault('rounds',[]).append({'ratings':self.rows('ratings',case_id),'decision':c.pop('decision',None),'revealed':c.pop('revealed',None),'why':why,'on':self.now()})
        c['reopened']=True
        with self.db:
            self.db.execute('delete from ratings where cid=?',(case_id,))
            self.put('cases',case_id,c)
    def note(self,case_id):
        c=self.get('cases',case_id)
        d=c.get('decision')
        if not d:raise ValueError('no panel decision yet')
        dl=date.fromisoformat(c['facts']['deadline'])
        dls=f'{dl.day} {dl:%B %Y}'
        facts={'route':d['route'],'prospects':d['band'],'alternative':d['alt'],'alternative_prospects':d['altband'],'reasons':[{'reason':r['text'],'view':v['view'],'why':v['line']} for r,v in zip(c['reasons'],d['views'])],'what_must_change':d['changes'],'homes_if_successful':d['homes'],'appeal_deadline':dls,'time_and_cost':{k:dict(zip(('preparation','decision','cost'),v)) for k,v in self.TIMING.items()},'rationale':d['rationale']}
        heads='Bottom line; Why; What would need to change; Time, cost and the appeal deadline; What we can help with next'+('; Why we advise stopping, and whether a different proposal could work' if d['route']=='stop' else '')
        sys='You write one-page triage notes for people whose small housing scheme was refused. Plain English, short sentences. You are independent of the council and cannot influence it. Never promise or imply success. Give prospects only in the exact words supplied. Use only the facts supplied.'
        for i in range(2):
            t=self.llm().ask(sys,f'Write the note under these headings: {heads}. State the appeal deadline date exactly as supplied.\n\nFACTS:\n'+json.dumps(facts))
            low=t.lower()
            bad=[w for w in ('guarantee','will win','certain to','definitely') if w in low]+[w for w in (d['band'],dls) if w.lower() not in low]
            if not bad:break
        if bad:raise ValueError(f'triage note failed its checks: {bad}')
        c['note']={'text':t,'model':self.llm().model,'on':self.now()}
        with self.db:self.put('cases',case_id,c)
        return t
    def calibrate(self,outcomes):
        o={(i,r):bool(s) for i,r,s in outcomes}
        p=lambda b:self.MID[self.BANDS.index(b)]
        brier=lambda xs:sum((q-y)**2 for q,y in xs)/len(xs) if xs else None
        panel,members,pairs,decided=[],{},[],[]
        for c in self.rows('cases'):
            d=c.get('decision')
            if not d:continue
            decided.append(d)
            rs=self.rows('ratings',c['case_id'])
            for route,band in ((d['route'],d['band']),(d['alt'],d['altband'])):
                if band is None or (c['case_id'],route) not in o:continue
                y=o[(c['case_id'],route)]
                panel.append((c['case_id'],route,band,p(band),y))
                own=[]
                for r in rs:
                    for rr,bb in ((r['route'],r['band']),(r['alt'],r['altband'])):
                        if rr==route and bb:
                            members.setdefault(r['member'],[]).append((p(bb),y))
                            own.append(p(bb))
                if own:pairs.append((sum(own)/len(own),p(band),y))
        bands={b:{'n':len(x),'predicted':self.MID[i],'observed':sum(x)/len(x)} for i,b in enumerate(self.BANDS) if (x:=[t[4] for t in panel if t[2]==b])}
        return {'n':len(panel),'panel':brier([(q,y) for *_,q,y in panel]),'members':{m:brier(x) for m,x in members.items()},'private':brier([(a,y) for a,_,y in pairs]),'agreed':brier([(b,y) for _,b,y in pairs]),'bands':bands,'misses':sorted(panel,key=lambda t:-(t[3]-t[4])**2)[:5],'stop':sum(d['route']=='stop' for d in decided)/len(decided) if decided else None}
    def run(self):
        new,notes,errors=self.intake(),[],[]
        for c in self.rows('cases'):
            if c.get('decision') and not c.get('note'):
                try:
                    self.note(c['case_id'])
                    notes.append(c['case_id'])
                except Exception as e:errors.append((c['case_id'],str(e)[:200]))
        session,fast=self.agenda()
        return {'intake':new,'session':session,'fast':fast,'notes':notes,'errors':errors}
class Outreach(Tools):
    RECIPIENTS=('agent','applicant','owner')
    CHANNELS=('post','email','phone')
    OUTCOMES=('reached','declined','noresponse','optedout','merged','nocontact')
    STEPS=('support','diy','stop')
    CONSENT=('support','tracking','followup','quotes','owner')
    FACTS=('owner','preapp','budget','finance','timescale','intent','who','language')
    INTENTS=('sell','rent','live','unknown')
    WHO=('owneroccupier','landlord','builder','selfbuilder','promoter','other')
    DOING=('applying','appealing','pd','other','selling','nothing','stopped')
    REASONS=('fee','reports','finance','buildcost','time','confidence','agent','unfixable','personal','sold')
    ANSWERS=('q1','q2','q3','q3other','q4','q5agent','q5preapp','q6')
    def __init__(self,panel,vault,store,today=None):
        os.makedirs(store,exist_ok=True)
        self.today=today or date.today()
        self.src=sqlite3.connect(f'file:{os.path.abspath(panel)}?mode=ro',uri=True)
        self.contacts=sqlite3.connect(f'file:{os.path.abspath(os.path.join(vault,"contacts.db"))}?mode=ro',uri=True)
        k=os.path.join(vault,'key')
        if not os.path.exists(k):
            fd=os.open(k,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            os.write(fd,os.urandom(32))
            os.close(fd)
        self.key=open(k,'rb').read()
        self.db=sqlite3.connect(os.path.join(vault,'outreach.db'))
        self.pub=sqlite3.connect(os.path.join(store,'interviews.db'))
        self.out=sqlite3.connect(os.path.join(store,'handoff.db'))
        with self.db:
            for t in ('cases','optouts','professionals'):self.db.execute(f'create table if not exists {t}(id text primary key,cid text,data text)')
        with self.pub:self.pub.execute('create table if not exists interviews(id text primary key,cid text,data text)')
        with self.out:self.out.execute('create table if not exists handoff(id text primary key,cid text,data text)')
    def now(self):return datetime.now().isoformat(timespec='seconds')
    def get(self,table,key,db=None):
        r=(db or self.db).execute(f'select data from {table} where id=?',(key,)).fetchone()
        return json.loads(r[0]) if r else None
    def put(self,table,key,row,db=None):(db or self.db).execute(f'insert or replace into {table} values(?,?,?)',(key,row.get('case_id',''),json.dumps(row,default=str)))
    def rows(self,table,db=None):return [json.loads(d) for (d,) in (db or self.db).execute(f'select data from {table} order by id')]
    def mac(self,s):return hmac.new(self.key,s.encode(),hashlib.sha256).hexdigest()[:32]
    def pseudo(self,case_id):return self.mac('case:'+case_id)
    def ident(self,name,address):
        if not name:return None
        a=(address or '').upper()
        pc=re.search(r'[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}',a)
        return self.mac('person:'+re.sub(r'[^A-Z0-9]','',name.upper())+'|'+re.sub(r'[^A-Z0-9]','',pc.group(0) if pc else a))
    def contact(self,case_id):
        r=self.contacts.execute('select data from contacts where id=?',(case_id,)).fetchone()
        return json.loads(r[0]) if r else {}
    def who(self,case_id,recipient):
        c=self.contact(case_id)
        if recipient=='agent':return self.ident(c.get('agent_company') or c.get('agent_name'),c.get('agent_address'))
        return self.ident(c.get(recipient+'_name'),c.get(recipient+'_address'))
    def variant(self,case_id):return 'AB'[int(self.mac('variant:'+case_id),16)%2]
    def day(self,r):return (self.today-date.fromisoformat(r['start'])).days
    def intake(self):
        have={r['case_id'] for r in self.rows('cases')}
        live={r['person']:r['case_id'] for r in self.rows('cases') if r.get('person') and not r.get('closed')}
        new=[c for c in (json.loads(d) for (d,) in self.src.execute('select data from cases order by id')) if c.get('decision') and c['case_id'] not in have]
        t=self.today.isoformat()
        with self.db:
            for c in new:
                f,p=c['facts'],self.who(c['case_id'],'applicant')
                r={'case_id':c['case_id'],'person':p,'start':c['decision']['on'][:10],'decided':f.get('decision_date'),'deadline':f.get('deadline'),'route':c['decision']['route'],'borough':f.get('borough'),'agent':bool(f.get('agent_named')),'certificate':f.get('certificate'),'variant':self.variant(c['case_id']),'attempts':[],'outcome':None,'consent':{}}
                if p is None and not r['agent']:r.update(outcome='nocontact',closed=t)
                elif p and self.get('optouts',p):r.update(outcome='optedout',closed=t)
                elif p in live:r.update(outcome='merged',merged=live[p],closed=t)
                elif p:live[p]=c['case_id']
                self.put('cases',c['case_id'],r)
        return [c['case_id'] for c in new]
    def due(self):
        out=[]
        for r in sorted(self.rows('cases'),key=lambda r:r['deadline'] or ''):
            if r['outcome'] is not None:continue
            d=self.day(r)
            if d>=28:
                out.append((r['case_id'],'close',None))
                continue
            for x in (['applicant'] if r['person'] else [])+(['agent'] if r['agent'] else []):
                w=sum(a['recipient']==x and a['channel']!='phone' for a in r['attempts'])
                if w==0:out.append((r['case_id'],'letter',x))
                elif w==1 and d>=14 and r.get('phase')!='backfill':out.append((r['case_id'],'reminder',x))
            if r['agent'] and d<=5 and self.contact(r['case_id']).get('agent_phone') and not any(a['channel']=='phone' for a in r['attempts']):out.append((r['case_id'],'phone','agent'))
        return out
    def site(self,case_id):
        r=self.src.execute('select data from cases where id=?',(case_id,)).fetchone()
        return json.loads(r[0])['facts'] if r else {}
    def letter(self,case_id,recipient):
        r,c=self.get('cases',case_id),self.contact(case_id)
        email=recipient=='agent' and c.get('agent_email')
        channel='email' if email else 'post'
        self.allowed(case_id,channel,recipient)
        written=sum(a['recipient']==recipient and a['channel']!='phone' for a in r['attempts'])
        kind='backfill' if r.get('phase')=='backfill' else 'reminder' if written else 'first'
        f=self.site(case_id)
        dl=date.fromisoformat(r['deadline']) if r.get('deadline') else None
        p={'reference':f.get('reference') or case_id.split(':',1)[1],'site':f.get('address') or '','deadline':f'{dl.day} {dl:%B %Y}' if dl else '','unit_phone':env('UNIT_PHONE',''),'unit_email':env('UNIT_EMAIL','')}
        via='EMAIL' if email else 'LETTER'
        tid=env(f'NOTIFY_{via}_{kind.upper()}_{r["variant"]}') or env(f'NOTIFY_{via}_{kind.upper()}')
        if not tid:raise ValueError(f'no Notify template for {via} {kind} {r["variant"]}')
        if email:body={'email_address':c['agent_email'],'template_id':tid,'personalisation':p,'reference':self.pseudo(case_id)}
        else:
            name=(c.get('agent_company') or c.get('agent_name')) if recipient=='agent' else c.get(recipient+'_name')
            raw=c.get(recipient+'_address') or ''
            pc=re.search(r'[A-Za-z]{1,2}\d[A-Za-z\d]?\s*\d[A-Za-z]{2}\b',raw)
            if not name or not pc:raise ValueError('a postal letter needs a name and a postcode')
            lines=[name]+[x.strip() for x in raw.replace(pc.group(0),'').split(',') if x.strip()][:5]+[pc.group(0).upper()]
            if len(lines)<3:raise ValueError('a postal letter needs at least three address lines')
            p.update({f'address_line_{i}':x for i,x in enumerate(lines,1)})
            body={'template_id':tid,'personalisation':p,'reference':self.pseudo(case_id)}
        res=self.tool('_notify',Notify).send('email' if email else 'letter',body)
        self.attempt(case_id,channel,recipient,ref=res.get('id'))
        return res.get('id')
    def allowed(self,case_id,channel,recipient):
        r=self.get('cases',case_id)
        if recipient not in self.RECIPIENTS or channel not in self.CHANNELS:raise ValueError((channel,recipient))
        if recipient!='agent' and channel!='post':raise ValueError('post only to individuals')
        if recipient=='agent' and not r['agent']:raise ValueError('no agent')
        if recipient=='owner' and not (r['consent'].get('owner') and r.get('certificate') in ('B','C','D')):raise ValueError('owner needs applicant consent')
        if recipient!='owner' and r['outcome'] is not None:raise ValueError(r['outcome'])
        p=self.who(case_id,recipient)
        if p and self.get('optouts',p):raise ValueError('opted out')
        sent=[a for a in r['attempts'] if a['recipient']==recipient]
        written=sum(a['channel']!='phone' for a in sent)
        if channel=='phone' and (any(a['channel']=='phone' for a in sent) or self.day(r)>5):raise ValueError('phone only once, days 0-5')
        if channel!='phone' and (written>=2 or written==1 and self.day(r)<14):raise ValueError('two written attempts, reminder from day 14')
        return r
    def attempt(self,case_id,channel,recipient,ref=None):
        r=self.allowed(case_id,channel,recipient)
        r['attempts'].append({'on':self.today.isoformat(),'channel':channel,'recipient':recipient,'ref':ref})
        with self.db:self.put('cases',case_id,r)
    def optout(self,case_id,recipient):
        p=self.who(case_id,recipient)
        r=self.get('cases',case_id)
        with self.db:
            if p:self.put('optouts',p,{'on':self.today.isoformat()})
            if recipient=='applicant':
                r.update(outcome='optedout',closed=self.today.isoformat())
                self.put('cases',case_id,r)
    def close(self,case_id,outcome):
        r=self.get('cases',case_id)
        if outcome not in self.OUTCOMES:raise ValueError(outcome)
        if outcome=='noresponse' and self.day(r)<28:raise ValueError('outreach runs to day 28')
        r.update(outcome=outcome,closed=self.today.isoformat())
        with self.db:self.put('cases',case_id,r)
    def conversation(self,case_id,caseworker,facts,step,consent):
        r=self.get('cases',case_id)
        if r['outcome'] not in (None,'reached'):raise ValueError(r['outcome'])
        if set(facts)!=set(self.FACTS) or facts['intent'] not in self.INTENTS or facts['who'] not in self.WHO or not isinstance(facts['owner'],bool) or not isinstance(facts['preapp'],bool):raise ValueError('facts')
        if step not in self.STEPS:raise ValueError(step)
        if set(consent)!=set(self.CONSENT) or not all(isinstance(v,bool) for v in consent.values()):raise ValueError('consent: each item asked separately')
        if step=='support' and not consent['support']:raise ValueError('support needs consent')
        r.update(outcome='reached',facts=facts,step=step,consent=consent,consented=self.today.isoformat(),caseworker=caseworker)
        r.setdefault('reached',self.today.isoformat())
        r.setdefault('days_left',(date.fromisoformat(r['deadline'])-self.today).days if r['deadline'] else None)
        if not any(consent.values()):r['closed']=self.today.isoformat()
        with self.db:self.put('cases',case_id,r)
    def interview(self,case_id,caseworker,answers,phase='live'):
        r=self.get('cases',case_id)
        a=answers
        if r['outcome'] not in ('reached','declined') and r.get('phase')!='backfill':raise ValueError(r['outcome'])
        if set(a)!=set(self.ANSWERS) or a['q2'] not in self.DOING or a['q4'] not in self.WHO or not isinstance(a['q5agent'],bool) or not isinstance(a['q5preapp'],bool):raise ValueError('answers')
        if (a['q2'] in ('stopped','nothing'))!=(a['q3'] is not None) or a['q3'] not in self.REASONS+(None,) or not set(a['q3other'])<=set(self.REASONS) or a['q3'] in a['q3other'] or a['q3'] is None and a['q3other']:raise ValueError('q3')
        pid=self.pseudo(case_id)
        with self.pub:self.put('interviews',pid,{'pid':pid,'borough':r['borough'],'answers':a,'coder':caseworker,'on':self.today.isoformat(),'phase':phase,'review':None},db=self.pub)
        r['interview']={k:a[k] for k in self.ANSWERS if k not in ('q1','q6')}
        if r.get('phase')=='backfill':r['outcome']='reached'
        with self.db:self.put('cases',case_id,r)
    def compare(self,pid):
        x=self.get('interviews',pid,db=self.pub)
        cid=next((r['case_id'] for r in self.rows('cases') if self.pseudo(r['case_id'])==pid),None)
        if not cid:raise KeyError(pid)
        p=self.src.execute('select data from cases where id=?',(cid,)).fetchone()
        rs=[r['text'] for r in json.loads(p[0]).get('reasons',[])] if p else []
        if not rs:raise ValueError('no coded reasons for this case')
        a=self.jev().ask({'applicant_account':x['answers']['q1'],'council_reasons':rs},{'understood':Jev.score("How well does the applicant's own account match the council's reasons for refusal?",['None: names none of the council reasons','Partly: names some but misses the main reason','Mostly: names the main reason','Fully: names every reason'])})['understood']
        x.update(understood=a['answer']>=2 if a['accepted'] else None,understanding=a)
        with self.pub:self.put('interviews',pid,x,db=self.pub)
        return x['understood']
    def review(self,rate=0.1,seed=None):
        seed=seed or self.today.isoformat()
        pool=[x for x in self.rows('interviews',db=self.pub) if x['review'] is None]
        pick=set(random.Random(seed).sample([x['pid'] for x in pool],math.ceil(len(pool)*rate)))
        with self.pub:
            for x in pool:
                x['review']='pending' if x['pid'] in pick else 'skip'
                self.put('interviews',x['pid'],x,db=self.pub)
        return sorted(pick)
    def reviewed(self,pid,reviewer,ok,notes=''):
        x=self.get('interviews',pid,db=self.pub)
        if x['review']!='pending':raise ValueError(x['review'])
        if reviewer==x['coder']:raise ValueError('same caseworker')
        x['review']={'by':reviewer,'ok':bool(ok),'notes':notes,'on':self.today.isoformat()}
        with self.pub:self.put('interviews',pid,x,db=self.pub)
    def roster(self,name,kind):
        with self.db:self.put('professionals',name,{'name':name,'kind':kind,'last':''})
    def professional(self,case_id,kind):
        ps=sorted((p for p in self.rows('professionals') if p['kind']==kind),key=lambda p:(p['last'],p['name']))
        if not ps:return None
        p,r=ps[0],self.get('cases',case_id)
        p['last']=self.now()
        r.setdefault('referred',[]).append({'name':p['name'],'kind':kind,'on':self.today.isoformat()})
        with self.db:
            self.put('professionals',p['name'],p)
            self.put('cases',case_id,r)
        return p['name']
    def equity(self):
        out={}
        for r in self.rows('cases'):
            if r['outcome'] in ('merged','nocontact'):continue
            f=r.get('facts') or {}
            for dim,val in (('borough',r['borough']),('agent',r['agent']),('type',f.get('who','unknown')),('language',bool(f.get('language')) if f else 'unknown')):
                g=out.setdefault(dim,{}).setdefault(str(val),{'n':0,'reached':0,'uptake':0})
                g['n']+=1
                g['reached']+=r['outcome'] in ('reached','declined')
                g['uptake']+=r.get('step')=='support'
        return out
    def measures(self):
        share=lambda a,b:a/b if b else None
        rs=[r for r in self.rows('cases') if r['outcome'] not in ('merged','nocontact')]
        sent=[r for r in rs if r['attempts']]
        quick=[r for r in sent if r['decided'] and (date.fromisoformat(min(a['on'] for a in r['attempts']))-date.fromisoformat(r['decided'])).days<=21]
        reached=[r for r in rs if r['outcome'] in ('reached','declined')]
        routes,variants={},{}
        for r in [r for r in reached if r.get('step')]:
            g=routes.setdefault(r['route'],[0,0])
            g[0]+=1
            g[1]+=r['step']=='support'
        for r in sent:
            g=variants.setdefault(r['variant'],[0,0])
            g[0]+=1
            g[1]+=r in reached
        return {'cases':len(rs),'contacted':len(sent),'within21':share(len(quick),len(sent)),'reached':share(len(reached),len(sent)),'uptake':{k:share(s,n) for k,(n,s) in routes.items()},'interviews':share(sum(bool(r.get('interview')) for r in reached),len(reached)),'optouts':sum(r['outcome']=='optedout' for r in rs),'variants':{k:share(s,n) for k,(n,s) in variants.items()}}
    def handoff(self):
        rs=[r for r in self.rows('cases') if r.get('step')=='support' and r['consent'].get('support')]
        with self.out:
            for r in rs:self.put('handoff',r['case_id'],{'case_id':r['case_id'],'route':r['route'],'reached':r['reached'],'consent':{k:r['consent'][k] for k in ('support','tracking','followup')},'caseworker':r['caseworker']},db=self.out)
        return [r['case_id'] for r in rs]
    def expired(self,days):return [r['case_id'] for r in self.rows('cases') if r.get('closed') and (self.today-date.fromisoformat(r['closed'])).days>=days]
    def backfill(self,cases,n,seed='phase1'):
        back={'won','undecided','refused','other'}
        pool={True:sorted({i for i,s in cases if s in back}),False:sorted({i for i,s in cases if s in ('none','changed')})}
        rng,t=random.Random(seed),self.today.isoformat()
        pick=[(i,k) for k in (True,False) for i in rng.sample(pool[k],min(len(pool[k]),n//2))]
        with self.db:
            for i,k in pick:
                if self.get('cases',i):continue
                p=self.who(i,'applicant')
                self.put('cases',i,{'case_id':i,'person':p,'start':t,'decided':None,'deadline':None,'route':None,'borough':i.split(':',1)[0],'agent':False,'certificate':None,'variant':self.variant(i),'attempts':[],'outcome':None if p and not self.get('optouts',p) else 'nocontact','consent':{},'phase':'backfill','came_back':k})
        return [i for i,_ in pick]
    def run(self):
        new,sent,manual,errors=self.intake(),[],[],[]
        for cid,action,who in self.due():
            try:
                if action=='close':self.close(cid,'noresponse')
                elif action=='phone':manual.append((cid,'phone',who))
                else:sent.append((cid,who,self.letter(cid,who)))
            except Exception as e:errors.append((cid,action,str(e)[:200]))
        return {'intake':new,'sent':sent,'manual':manual,'handoff':self.handoff(),'errors':errors}
class Support(Tools):
    PLAYBOOK={'redesign':'B','appeal':'C','alternative':'D','preapp':'E','stop':'F'}
    ROLES=('planner','senior')
    KINDS=('planner','architect','specialist','solicitor','fee','cil')
    CAPS={'architect':5,'specialist':1}
    CALLOFF=('architect','daylight','highways','trees','ecology','heritage','drainage','solicitor')
    TICKS=('validation','letter','applicant','fee','bng','fire','obligation')
    DAILY=('bng','fire')
    FULL=('status','matrix','fresh','validation','letter','applicant','fee','bng','fire')
    REQUIRE={'application':FULL,'pip':FULL,'prior':FULL,'ldc':('status','applicant','fee'),'appeal':('status','matrix','fresh','letter','applicant','senior','eligibility','deadline','obligation'),'preapp':('status','applicant')}
    RESULTS=('approved','refused','allowed','dismissed','withdrawn','advice')
    CLOSE=('stop','applicant','cap','finance')
    def __init__(self,panel,handoff,store,today=None,lottery=None):
        os.makedirs(store,exist_ok=True)
        self.today=today or date.today()
        self.lottery=sqlite3.connect(f'file:{os.path.abspath(lottery)}?mode=ro',uri=True) if lottery else None
        self.panel=sqlite3.connect(f'file:{os.path.abspath(panel)}?mode=ro',uri=True)
        self.handoff=sqlite3.connect(f'file:{os.path.abspath(handoff)}?mode=ro',uri=True)
        self.db=sqlite3.connect(os.path.join(store,'support.db'))
        with self.db:
            for t in ('cases','staff','roster','precedents'):self.db.execute(f'create table if not exists {t}(id text primary key,cid text,data text)')
    def now(self):return datetime.now().isoformat(timespec='seconds')
    def get(self,table,key):
        r=self.db.execute(f'select data from {table} where id=?',(key,)).fetchone()
        return json.loads(r[0]) if r else None
    def put(self,table,key,row):self.db.execute(f'insert or replace into {table} values(?,?,?)',(key,row.get('case_id',''),json.dumps(row,default=str)))
    def rows(self,table):return [json.loads(d) for (d,) in self.db.execute(f'select data from {table} order by id')]
    def save(self,c):
        with self.db:self.put('cases',c['case_id'],c)
    def ref(self,x):return re.sub(r'\s+','',(x or '').upper())
    def days(self,c):return (date.fromisoformat(c['deadline'])-self.today).days
    def playbook(self,route,reasons):
        if route in ('resubmit','resubmit_appeal'):return 'A' if all(r['category']=='E' for r in reasons) else 'B'
        return self.PLAYBOOK[route]
    def apply(self,c,p):
        d=p['decision']
        c.update(route=d['route'],playbook=self.playbook(d['route'],p['reasons']),band=d['band'],homes=d['homes'],reasons=p['reasons'],views=d['views'],deadline=p['facts'].get('deadline'),borough=p['facts'].get('borough'),decided=d['on'],ticks={},version=c.get('version',0)+1,status='open')
        c.setdefault('original',p['facts'].get('homes_net'))
        c['matrix']=[{'n':r['n'],'text':r['text'],'code':r['theme']+'/'+r['category'],'changed':None,'evidence':None,'policy':None,'risk':None,'by':None} for r in p['reasons']]
        if c['refusals']>=2 and Panel.BANDS.index(d['band'])<3:c.update(status='closed',closed={'why':'two supported attempts','on':self.today.isoformat()})
        return c
    def intake(self):
        panel=[json.loads(d) for (d,) in self.panel.execute('select data from cases order by id')]
        byid={p['case_id']:p for p in panel}
        won=self.winners()
        new=[]
        for (d,) in self.handoff.execute('select data from handoff order by id'):
            h=json.loads(d)
            p=byid.get(h['case_id'])
            if self.get('cases',h['case_id']) or not p or not p.get('decision') or won is not None and h['case_id'] not in won:continue
            self.save(self.apply({'case_id':h['case_id'],'reached':h['reached'],'consent':h['consent'],'refusals':0,'submissions':[],'spend':[],'signoffs':[],'links':[],'compared':[],'feedback':[],'taken':self.now()},p))
            new.append(h['case_id'])
        subs={self.ref(s['reference']):c['case_id'] for c in self.rows('cases') for s in c['submissions'] if s['reference']}
        for p in panel:
            if not p.get('decision'):continue
            c=self.get('cases',p['case_id']) or self.get('cases',subs.get(self.ref(p['facts'].get('resubmission_of')),''))
            if not c or c['status']!='returned' or p['decision']['on']<=c['returned']:continue
            if p['case_id']!=c['case_id']:
                if p['case_id'] in c['links']:continue
                c['links'].append(p['case_id'])
                c['compared'].append(self.compare(c['reasons'],p['reasons']))
            self.save(self.apply(c,p))
        return new
    def winners(self):return {i for (b,) in self.lottery.execute('select body from ledger order by seq') for e in [json.loads(b)] if e['kind']=='lottery' for i in e['data']['winners']} if self.lottery else None
    def staff(self,name,role):
        if role not in self.ROLES:raise ValueError(role)
        with self.db:self.put('staff',name,{'name':name,'role':role})
    def senior(self,name):return (self.get('staff',name) or {}).get('role')=='senior'
    def worked(self,c):return ({c.get('planner')}|{r['by'] for r in c['matrix']}|{s['by'] for s in c['spend']})-{None}
    def assign(self,case_id,planner):
        if not self.get('staff',planner):raise KeyError(planner)
        c=self.get('cases',case_id)
        c['planner']=planner
        self.save(c)
    def confirm(self,case_id,ownership,finance,commitment,by):
        c=self.get('cases',case_id)
        ok=ownership is True and finance is True and commitment is True
        c['confirmed']={'ownership':ownership,'finance':finance,'commitment':commitment,'by':by,'on':self.today.isoformat()}
        if c['status'] in ('open','paused'):c['status']='open' if ok else 'paused'
        self.save(c)
        return ok
    def roster(self,name,kind):
        if kind not in self.CALLOFF:raise ValueError(kind)
        with self.db:self.put('roster',name,{'name':name,'kind':kind,'last':''})
    def calloff(self,case_id,kind):
        ps=sorted((p for p in self.rows('roster') if p['kind']==kind),key=lambda p:(p['last'],p['name']))
        if not ps:return None
        p,c=ps[0],self.get('cases',case_id)
        p['last']=self.now()
        c.setdefault('calloffs',[]).append({'name':p['name'],'kind':kind,'on':self.today.isoformat()})
        with self.db:
            self.put('roster',p['name'],p)
            self.put('cases',case_id,c)
        return p['name']
    def used(self,c,kind):
        u=[s for s in c['spend'] if s['payer']=='unit' and s['kind']==kind]
        return sum(s['qty'] for s in u) if kind=='architect' else len(u)
    def signed(self,c,why):return any(s['why']==why for s in c['signoffs'])
    def signoff(self,case_id,senior,why):
        if why not in ('appeal','cap'):raise ValueError(why)
        if not self.senior(senior):raise ValueError('senior sign-off only')
        c=self.get('cases',case_id)
        c['signoffs'].append({'by':senior,'why':why,'on':self.today.isoformat()})
        self.save(c)
    def spend(self,case_id,kind,qty,cost,payer,by):
        c=self.get('cases',case_id)
        if kind not in self.KINDS or payer not in ('unit','applicant'):raise ValueError((kind,payer))
        if kind in ('fee','cil') and payer!='applicant':raise ValueError('the applicant pays the fee and CIL')
        if c['status']!='open' or not all((c.get('confirmed') or {}).get(k) is True for k in ('ownership','finance','commitment')):raise ValueError('checkpoint: ownership, finance and commitment first')
        if payer=='unit' and kind in self.CAPS and self.used(c,kind)+(qty if kind=='architect' else 1)>self.CAPS[kind] and not self.signed(c,'cap'):raise ValueError('cap reached: applicant funds the rest, senior sign-off, or close')
        c['spend'].append({'kind':kind,'qty':qty,'cost':cost,'payer':payer,'by':by,'on':self.today.isoformat()})
        self.save(c)
    def respond(self,case_id,n,changed,evidence,policy,risk,by):
        c=self.get('cases',case_id)
        if not all(isinstance(x,str) and x.strip() for x in (changed,evidence,policy,risk)):raise ValueError('every column filled')
        r=next(r for r in c['matrix'] if r['n']==n)
        r.update(changed=changed,evidence=evidence,policy=policy,risk=risk,by=by)
        c['version']+=1
        self.save(c)
    def matrix(self,case_id):return self.get('cases',case_id)['matrix']
    def facts(self,case_id):
        r=self.panel.execute('select data from cases where id=?',(case_id,)).fetchone()
        return json.loads(r[0])['facts'] if r else {}
    def letter(self,case_id):
        c=self.get('cases',case_id)
        if any(not r[k] for r in c['matrix'] for k in ('changed','evidence','policy','risk')):raise ValueError('complete the matrix first')
        f=self.facts(case_id)
        facts={'site':f.get('address'),'refused_reference':f.get('reference'),'refused_on':f.get('decision_date'),'earlier_applications':f.get('earlier_refs'),'supported_refusals_so_far':c['refusals'],'matrix':[{k:r[k] for k in ('n','text','code','changed','evidence','policy','risk')} for r in c['matrix']]}
        t=self.llm().ask('You draft covering letters for resubmitted small housing schemes in London, for the applicant or their agent to send under their own name. Formal, plain and factual. Never claim the scheme will be approved.','Draft the covering letter: the site history, then a section for each refusal reason headed "Reason N" saying what changed and the evidence, then the reasons-response matrix as a table.\n\nFACTS:\n'+json.dumps(facts,default=str))
        missing=[r['n'] for r in c['matrix'] if f'reason {r["n"]}' not in t.lower()]
        if missing:raise ValueError(f'the letter does not answer reasons {missing}')
        c['letter']={'text':t,'version':c['version'],'on':self.now()}
        self.save(c)
        return t
    def statement(self,case_id):
        c=self.get('cases',case_id)
        if c['playbook']!='C':raise ValueError('appeal cases only')
        f=self.facts(case_id)
        src=[Pdf.text(x) for k in ('notice','report') for x in (f.get('files') or {}).get(k,[]) if os.path.exists(x)]
        if not src:raise ValueError('no council documents on file')
        text='\n\n'.join(src)
        t=self.llm().ask("You draft statements of case for written-representations planning appeals in England. Use only the council's own material supplied, quoting it exactly in double quotation marks. Add no new evidence.",'For each refusal reason, show where the officer report, consultee responses or the cited policy undermine it, quoting the source exactly.\n\nREASONS:\n'+json.dumps([{'n':r['n'],'text':r['text']} for r in c['reasons']])+'\n\nCOUNCIL MATERIAL:\n'+text[:150000])
        quotes=re.findall(r'["“]([^"”]{12,})["”]',t)
        bad=[q for q in quotes if not Pdf.verbatim(q,text) and not any(Pdf.verbatim(q,r['text']) for r in c['reasons'])]
        if bad:raise ValueError(f'{len(bad)} quotations are not in the council material')
        c['statement']={'text':t,'quotes':len(quotes),'version':c['version'],'on':self.now()}
        self.save(c)
        return t
    def review(self,case_id,reviewer,ok,notes=''):
        c=self.get('cases',case_id)
        if not self.senior(reviewer):raise ValueError('reviewer must be senior')
        if reviewer in self.worked(c):raise ValueError('reviewer has worked on the case')
        c['review']={'by':reviewer,'ok':bool(ok),'notes':notes,'version':c['version'],'on':self.today.isoformat()}
        self.save(c)
    def tick(self,case_id,item,by,value='met'):
        if item not in self.TICKS or value not in ('met','na'):raise ValueError((item,value))
        c=self.get('cases',case_id)
        c['ticks'][item]={'value':value,'by':by,'on':self.today.isoformat()}
        self.save(c)
    def checks(self,case_id,kind):
        if kind not in self.REQUIRE:raise ValueError(kind)
        c=self.get('cases',case_id)
        t,f=c['ticks'],set()
        if c['status']!='open':f.add('status')
        if any(not r[k] for r in c['matrix'] for k in ('changed','evidence','policy','risk')):f.add('matrix')
        if not c.get('review') or not c['review']['ok'] or c['review']['version']!=c['version']:f.add('fresh')
        f|={k for k in ('validation','letter','applicant','fee','obligation') if k not in t}
        f|={k for k in self.DAILY if t.get(k,{}).get('on')!=self.today.isoformat()}
        if not self.signed(c,'appeal'):f.add('senior')
        v={x['n']:x['view'] for x in c['views']}
        if any(v[r['n']]!='weak' and r['theme']!='agreement' for r in c['reasons']):f.add('eligibility')
        if self.days(c)<0:f.add('deadline')
        if not any(r['theme']=='agreement' for r in c['reasons']):f.discard('obligation')
        return sorted(f&set(self.REQUIRE[kind]))
    def submit(self,case_id,reference,kind,by):
        if by not in ('applicant','agent'):raise ValueError('the applicant or agent submits, never the unit')
        f=self.checks(case_id,kind)
        if f:raise ValueError(f)
        c=self.get('cases',case_id)
        c['submissions'].append({'reference':reference,'kind':kind,'by':by,'on':self.today.isoformat(),'playbook':c['playbook'],'band':c['band'],'result':None})
        if kind!='preapp':c['status']='submitted'
        self.save(c)
    def outcome(self,case_id,result,homes=None,conditions=(),cil=None,notes=''):
        if result not in self.RESULTS:raise ValueError(result)
        if result in ('approved','allowed') and not isinstance(homes,int):raise ValueError('homes consented')
        c=self.get('cases',case_id)
        s=next(s for s in reversed(c['submissions']) if s['result'] is None)
        if (s['kind']=='preapp')!=(result=='advice'):raise ValueError((s['kind'],result))
        s.update(result=result,decision=self.today.isoformat(),homes=homes,notes=notes)
        if result in ('approved','allowed'):c.update(status='consented',consented=homes,conditions=list(conditions),cil=cil)
        elif result in ('refused','dismissed'):
            c['refusals']+=1
            c.update(status='returned',returned=self.now())
        elif result=='withdrawn':c['status']='open'
        self.save(c)
    def rerate(self,case_id,why):
        c=self.get('cases',case_id)
        c.update(status='returned',returned=self.now(),rerate=why)
        self.save(c)
    def compare(self,old,new):
        a,b={r['theme'] for r in old},{r['theme'] for r in new}
        return {'same':sorted(a&b),'new':sorted(b-a),'fixed':sorted(a-b)}
    def precedent(self,case_id,reference,issues):
        c=self.get('cases',case_id)
        s=next(s for s in c['submissions'] if s['reference']==reference and s['kind']=='appeal' and s['result'] in ('allowed','dismissed'))
        if not issues or any(t not in Coding.THEMES or k not in Coding.CATEGORIES or not isinstance(u,bool) for t,k,u in issues):raise ValueError('issues')
        with self.db:self.put('precedents',self.ref(reference),{'case_id':case_id,'reference':reference,'borough':c['borough'],'result':s['result'],'issues':[{'theme':t,'category':k,'upheld':u} for t,k,u in issues],'on':s['decision']})
    def close(self,case_id,why,by,notes=''):
        if why not in self.CLOSE:raise ValueError(why)
        c=self.get('cases',case_id)
        c.update(status='closed',closed={'why':why,'by':by,'notes':notes,'on':self.today.isoformat()})
        self.save(c)
    def satisfaction(self,case_id,who,score):
        if who not in ('applicant','agent') or score not in (1,2,3,4,5):raise ValueError((who,score))
        c=self.get('cases',case_id)
        c['feedback'].append({'who':who,'score':score,'on':self.today.isoformat()})
        self.save(c)
    def measures(self):
        share=lambda a,b:a/b if b else None
        mean=lambda xs:sum(xs)/len(xs) if xs else None
        mid=lambda s:Panel.MID[Panel.BANDS.index(s['band'])]
        cs=self.rows('cases')
        subs=[s for c in cs for s in c['submissions'] if s['result'] in ('approved','refused','allowed','dismissed')]
        apps,appeals=[s for s in subs if s['kind']!='appeal'],[s for s in subs if s['kind']=='appeal']
        by={}
        for c in cs:
            g=by.setdefault(c['playbook'],[0,0])
            g[0]+=1
            g[1]+=c.get('consented') or 0
        cost=sum(x['cost'] for c in cs for x in c['spend'] if x['payer']=='unit')
        homes=sum(c.get('consented') or 0 for c in cs)
        wait=[(date.fromisoformat(s[0]['on'])-date.fromisoformat(c['reached'])).days for c in cs if (s:=[s for s in c['submissions'] if s['kind']!='preapp'])]
        return {'cases':len(cs),'homes_per_case':{k:h/n for k,(n,h) in sorted(by.items())},'homes_lost':sum((c['original'] or 0)-c['consented'] for c in cs if c.get('consented') is not None),'resubmissions':{'approved':share(sum(s['result']=='approved' for s in apps),len(apps)),'predicted':mean([mid(s) for s in apps]),'n':len(apps)},'appeals':{'allowed':share(sum(s['result']=='allowed' for s in appeals),len(appeals)),'predicted':mean([mid(s) for s in appeals]),'national':Panel.BASE['appeals'][0],'n':len(appeals)},'same_reason':sum(bool(x['same']) for c in cs for x in c['compared']),'new_reason':sum(bool(x['new']) for c in cs for x in c['compared']),'days_to_submit':statistics.median(wait) if wait else None,'cost':cost,'cost_per_case':share(cost,len(cs)),'cost_per_home':share(cost,homes),'satisfaction':{w:mean([f['score'] for c in cs for f in c['feedback'] if f['who']==w]) for w in ('applicant','agent')},'stop':share(sum((c.get('closed') or {}).get('why')=='stop' for c in cs),len(cs))}
    def run(self):
        new,cs=self.intake(),self.rows('cases')
        return {'intake':new,'status':{s:sum(c['status']==s for c in cs) for s in sorted({c['status'] for c in cs})},'at_cap':[c['case_id'] for c in cs if any(self.used(c,k)>=v for k,v in self.CAPS.items()) and not self.signed(c,'cap')],'appeal_deadlines':sorted((c['deadline'],c['case_id']) for c in cs if c['playbook']=='C' and c['status'] in ('open','paused'))}
class Tracking(Tools):
    GROUPS=('supported','triaged','untriaged','comparison','historical')
    RANK={'untriaged':0,'triaged':1,'supported':2}
    POINTS={'m6':6,'m12':12,'m24':24,'m36':36}
    CHECKS=('m6','m12','m24','m36','monthly','quarterly','trace')
    KINDS=('application','decision','appeal','appeal_decision','costs','householder','sold','discharge','started','completed')
    ROUTES=('full','outline','reserved','prior','pip','tdc','ldc')
    WON=('full','outline','reserved','prior','tdc')
    VALID={'decision':('approved','refused','withdrawn'),'appeal_decision':('allowed','dismissed','withdrawn'),'costs':('appellant','council')}
    SOURCES=('register','pins','landregistry','datahub','buildingcontrol','cil','voa','epc','visit')
    REQUIRED=('register','pins')
    RULES=('uprn','redline','history','address')
    CONFIDENCE=('high','medium','low')
    STATES=('won','pending','undecided','refused','changed','none','gap','other')
    def __init__(self,store,register=None,panel=None,support=None,today=None):
        os.makedirs(store,exist_ok=True)
        self.today=today or date.today()
        ro=lambda p:sqlite3.connect(f'file:{os.path.abspath(p)}?mode=ro',uri=True) if p else None
        self.register,self.panel,self.support=ro(register),ro(panel),ro(support)
        self.db=sqlite3.connect(os.path.join(store,'tracking.db'))
        with self.db:
            for t in ('cases','events','checks','snapshots'):self.db.execute(f'create table if not exists {t}(id text primary key,cid text,data text)')
    def get(self,table,key):
        r=self.db.execute(f'select data from {table} where id=?',(key,)).fetchone()
        return json.loads(r[0]) if r else None
    def put(self,table,key,row):self.db.execute(f'insert or replace into {table} values(?,?,?)',(key,row.get('case_id',''),json.dumps(row,default=str)))
    def rows(self,table,cid=None):
        q=self.db.execute(f'select data from {table} order by id') if cid is None else self.db.execute(f'select data from {table} where cid=? order by id',(cid,))
        return [json.loads(d) for (d,) in q]
    def save(self,c):
        with self.db:self.put('cases',c['case_id'],c)
    def months(self,d,n):
        d=date.fromisoformat(d) if isinstance(d,str) else d
        y,m=d.year+(d.month+n-1)//12,(d.month+n-1)%12+1
        return date(y,m,min(d.day,calendar.monthrange(y,m)[1]))
    def enrol(self,case_id,group,borough,refused,address=None,uprn=None,homes=None,category=None,tracking=False,active=False):
        if group not in self.GROUPS:raise ValueError(group)
        date.fromisoformat(refused)
        c=self.get('cases',case_id)
        if c and c['group']!=group and (c['group'],group) not in {('untriaged','triaged'),('untriaged','supported'),('triaged','supported')}:raise ValueError((c['group'],group))
        c=c or {'case_id':case_id,'documents':[],'timeline':None,'groups':[]}
        if c.get('group')!=group:c['groups'].append({'group':group,'on':self.today.isoformat()})
        c.update(group=group,borough=borough,refused=refused,address=address,uprn=uprn,homes=homes,category=category,tracking=bool(tracking),active=bool(active))
        self.save(c)
    def intake(self):
        panel={p['case_id']:p for p in (json.loads(d) for (d,) in self.panel.execute('select data from cases order by id')) if p.get('decision')} if self.panel else {}
        src=[('untriaged',json.loads(d),None) for (d,) in self.register.execute('select data from register order by id')] if self.register else []
        src+=[('triaged',p['facts'],None) for p in panel.values()]
        src+=[('supported',panel[s['case_id']]['facts'],s) for s in (json.loads(d) for (d,) in self.support.execute('select data from cases order by id')) if s['case_id'] in panel] if self.support else []
        new=[]
        for g,f,s in src:
            c=self.get('cases',f['case_id'])
            if c and (c['group'] not in self.RANK or self.RANK[c['group']]>self.RANK[g]):continue
            if not c:new.append(f['case_id'])
            p=panel.get(f['case_id']) or {}
            self.enrol(f['case_id'],g,f.get('borough'),f.get('decision_date'),f.get('address'),f.get('uprn'),f.get('homes_net'),p.get('category'),bool(s and s['consent'].get('tracking')),bool(s and s['status']!='closed'))
        return new
    PPD=('tuid','price','date','postcode','type','new','duration','paon','saon','street','locality','town','district','county','category','status')
    APPLICATIONS={'full':'A full planning application for homes','outline':'An outline application for homes','reserved':'Reserved matters after outline permission','prior':'Prior approval for homes under permitted development','pip':'Permission in Principle','tdc':'Technical details consent after Permission in Principle','ldc':'A lawful development certificate for homes','householder':'Householder works, or a certificate for an extension','other':'Not about homes on this site'}
    def idox(self,borough):
        h,p=Register.BOROUGHS.get(borough,(env('REGISTER_HOST_'+borough.upper()),env('REGISTER_PLATFORM_'+borough.upper())))
        if p!='idox' or not h:raise NotImplementedError(f'{borough}: register platform not confirmed')
        return self.tool('_idox_'+borough,lambda:Idox(h,Web(env('RAW_DIR','raw'))))
    def web(self):return self.tool('_web',lambda:Web(env('RAW_DIR','raw')))
    def registered(self,c):
        ix,ref=self.idox(c['borough']),c['case_id'].split(':',1)[1]
        url,seen=ix.find(ref),{}
        if url:
            for a in ix.related(url)['applications']:seen[a['reference']]=(a,'history')
        if c.get('address'):
            for a in ix.simple(c['address']):seen.setdefault(a['reference'],(a,'address'))
        k0,out=re.sub(r'\s+','',ref.upper()),[]
        for r,(a,rule) in seen.items():
            if re.sub(r'\s+','',r.upper())==k0:continue
            s=ix.labels(a['url'])
            got=when(s.get('application received') or s.get('application validated'))
            if not got or got<c['refused']:continue
            out.append({'source':'register','kind':'application','ref':r,'description':a.get('description') or s.get('proposal',''),'address':s.get('address') or a.get('address',''),'on':got,'decision':s.get('decision'),'decided':when(s.get('decision issued date') or s.get('decision date')),'rule':rule})
        return out
    def acp(self,n):
        from selectolax.lexbor import LexborHTMLParser as HTMLParser
        x=re.sub(r'\s+',' ',HTMLParser(self.web().get(env('ACP_URL','https://acp.planninginspectorate.gov.uk/ViewCase.aspx?caseid={n}').format(n=n)).text).text())
        g=lambda p:(m.group(1) if (m:=re.search(p,x,re.I)) else None)
        return {'decision':g(r'Decision\s*:?\s*(Allowed|Dismissed|Split Decision|Withdrawn|Turned Away)'),'decided':when(g(r'Decision Date\s*:?\s*(\d{1,2} \w+ \d{4}|\d{2}/\d{2}/\d{4})')),'on':when(g(r'Start Date\s*:?\s*(\d{1,2} \w+ \d{4}|\d{2}/\d{2}/\d{4})'))}
    def appealed(self,c):
        ix,ref=self.idox(c['borough']),c['case_id'].split(':',1)[1]
        url=ix.find(ref)
        if not url:return []
        s=ix.labels(url)
        if not (s.get('appeal status') or s.get('appeal decision')):return []
        a=s.get('appeal reference') or s.get('appeal ref') or ref+'/appeal'
        x={'source':'register','kind':'appeal','ref':a,'on':when(s.get('appeal lodged date') or s.get('appeal received date') or s.get('appeal start date')),'decision':s.get('appeal decision'),'decided':when(s.get('appeal decision date')),'rule':'history','homes':c.get('homes')}
        out=[x]
        m=re.search(r'(\d{7})',a)
        if m:out.append({**x,**{k:v for k,v in self.acp(m.group(1)).items() if v},'source':'pins'})
        return out
    def sales(self,c):
        if '_ppd' not in self.__dict__:self._ppd=(Lookup(env('PPD_FILE'),'tuid',('date','postcode','paon','saon'),fields=self.PPD),Lookup(env('PPD_FILE'),'postcode',('tuid','date','paon','saon'),many=True,fields=self.PPD),Lookup(env('PPD_UPRN'),env('PPD_UPRN_KEY','uprn'),(env('PPD_UPRN_TUID','transaction_unique_identifier'),),many=True) if env('PPD_UPRN') else None)
        by,pc,up=self._ppd
        out,seen=[],set()
        if c.get('uprn') and up:
            for r in up.get(c['uprn']):
                t=str(r[env('PPD_UPRN_TUID','transaction_unique_identifier').lower()]).strip('{}')
                s=by.get(t) or by.get('{'+t+'}')
                if s and s['date'][:10]>=c['refused']:
                    seen.add(t)
                    out.append({'source':'landregistry','kind':'sold','ref':t,'on':s['date'][:10],'rule':'uprn'})
        m=re.search(r'([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})',(c.get('address') or '').upper())
        num=re.match(r'\s*(?:FLAT \w+,?\s*)?([\w-]+)',(c.get('address') or '').upper())
        if m and num:
            for r in pc.get(f'{m.group(1)} {m.group(2)}'):
                t=str(r['tuid']).strip('{}')
                if t not in seen and r['date'][:10]>=c['refused'] and num.group(1) in (str(r['paon']).upper(),str(r['saon']).upper()):out.append({'source':'landregistry','kind':'sold','ref':t,'on':r['date'][:10],'rule':'address','address':f"{r['saon']} {r['paon']}".strip()})
        return out
    def hub(self,c):
        if not c.get('uprn'):return []
        f={k:env('PLD_FIELD_'+k.upper(),v) for k,v in (('ref','lpa_app_no'),('uprn','uprn'),('start','actual_start_date'),('completion','actual_completion_date'))}
        r=self.web().post(env('PLD_URL','https://planningdata.london.gov.uk/api-guest/applications/_search'),json={'size':100,'query':{'term':{f['uprn']:str(c['uprn'])}}},headers={'X-API-AllowRequest':env('PLD_GUEST_KEY','')})
        out=[]
        for h in r.json().get('hits',{}).get('hits',[]):
            s=h['_source']
            for k,kind in (('start','started'),('completion','completed')):
                d=when(s.get(f[k]))
                if d and d>=c['refused']:out.append({'source':'datahub','kind':kind,'ref':str(s.get(f['ref'],'')),'on':d,'rule':'uprn'})
        return out
    def certificates(self,c):
        if not c.get('uprn'):return []
        r=self.web().get(env('EPC_URL','https://epc.opendatacommunities.org/api/v1/domestic/search'),params={'uprn':c['uprn'],'size':100},headers={'Authorization':env('EPC_AUTH',''),'Accept':'application/json'})
        rows=r.json().get('rows',[]) if r.content else []
        return [{'source':'epc','kind':'completed','ref':str(x.get('lmk-key')),'on':str(x.get('lodgement-date'))[:10],'rule':'uprn'} for x in rows if 'new dwelling' in str(x.get('transaction-type','')).lower() and str(x.get('lodgement-date'))[:10]>=c['refused']]
    def search(self,case_id,source):
        c=self.get('cases',case_id)
        f={'register':self.registered,'pins':self.appealed,'landregistry':self.sales,'datahub':self.hub,'epc':self.certificates}.get(source)
        if not f:raise ValueError(f'{source} is checked by hand: record what you find with event()')
        return f(c)
    def events(self,x,c):
        k=x['kind']
        if k in ('sold','started','completed'):return [{'kind':k,'on':x['on'],'source':x['source'],'ref':x['ref']}] if x.get('on') else []
        if k=='appeal':
            d=str(x.get('decision') or '').lower()
            o='allowed' if 'allow' in d else 'dismissed' if 'dismiss' in d else 'withdrawn' if 'withdraw' in d else None
            on=x.get('on') or x.get('decided')
            out=[{'kind':'appeal','on':on,'source':x['source'],'ref':x['ref']}] if on else []
            if o and x.get('decided'):out.append({'kind':'appeal_decision','on':x['decided'],'source':x['source'],'ref':x['ref'],'outcome':o,'homes':x.get('homes') if o=='allowed' else None})
            return out
        if not x.get('on'):return []
        q=self.jev().ask(str(x.get('description','')),{'route':Jev.choice('What kind of application is this?',self.APPLICATIONS)})['route']
        if not q['accepted']:raise ValueError('application type unclear')
        r=q['answer']
        if r=='other':return []
        if r=='householder':return [{'kind':'householder','on':x['on'],'source':x['source'],'ref':x['ref']}]
        out=[{'kind':'application','on':x['on'],'source':x['source'],'ref':x['ref'],'route':r}]
        d=str(x.get('decision') or '').lower()
        o='refused' if 'refus' in d else 'withdrawn' if 'withdraw' in d else 'approved' if any(w in d for w in ('grant','approv','permit','allowed')) else None
        if o and x.get('decided'):out.append({'kind':'decision','on':x['decided'],'source':x['source'],'ref':x['ref'],'route':r,'outcome':o,'homes':x.get('homes')})
        return out
    def link(self,case_id,candidates):
        c,made,people=self.get('cases',case_id),[],[]
        for x in candidates:
            rule,conf,q=x.get('rule'),'high',None
            if rule not in ('uprn','history'):
                q=self.jev().ask({'refused_site':c.get('address'),'candidate_address':x.get('address'),'candidate_description':x.get('description')},{'same':Jev.noul('Is the candidate on the same site as the refused scheme?','The same plot of land, even if the address is written differently, such as land behind a house or a new number for the same plot','A different site')})['same']
                if q['p']<0.5:continue
                rule,conf='address','low'
            try:es=self.events(x,c)
            except ValueError as er:
                people.append({'ref':x.get('ref'),'why':str(er)})
                continue
            for e in es:
                try:made.append(self.event(case_id,by='link',rule=rule,confidence=conf,machine=q,**e))
                except (ValueError,KeyError) as er:people.append({'ref':x.get('ref'),'kind':e['kind'],'why':str(er)})
        return {'events':made,'for_people':people}
    def overlap(self,a,b):
        from shapely import wkt
        from shapely.geometry import shape
        g=lambda x:wkt.loads(x) if isinstance(x,str) else shape(x) if isinstance(x,dict) else x
        a,b=g(a),g(b)
        m=min(a.area,b.area)
        return a.intersection(b).area/m if m else 0.0
    def timeline(self,case_id,apps,by):
        if any(not a.get('ref') or a.get('on','')<'2015-01-01' for a in apps):raise ValueError('applications since 2015, each with its reference')
        c=self.get('cases',case_id)
        c['timeline']={'apps':sorted(apps,key=lambda a:a['on']),'by':by,'on':self.today.isoformat()}
        self.save(c)
    def capture(self,case_id,ref,path):
        c=self.get('cases',case_id)
        c['documents'].append({'ref':ref,'path':path,'on':self.today.isoformat()})
        self.save(c)
    def event(self,case_id,kind,on,source,ref,by,rule,confidence,route=None,outcome=None,homes=None,limit=3,machine=None):
        c=self.get('cases',case_id)
        if c is None:raise KeyError(case_id)
        if kind not in self.KINDS or source not in self.SOURCES or rule not in self.RULES or confidence not in self.CONFIDENCE or not ref:raise ValueError((kind,source,rule,confidence,ref))
        if (kind in ('application','decision'))!=(route is not None) or route not in self.ROUTES+(None,):raise ValueError(('route',route))
        if outcome not in self.VALID.get(kind,(None,)):raise ValueError((kind,outcome))
        if (kind=='decision' and outcome=='approved' or kind=='appeal_decision' and outcome=='allowed') and not isinstance(homes,int):raise ValueError('net homes consented')
        if rule=='address' and confidence!='low':raise ValueError('an address match alone needs a second checker')
        if on<c['refused']:raise ValueError('before the refusal: that belongs in the timeline')
        k='|'.join((case_id,kind,source,ref))
        row={'key':k,'case_id':case_id,'kind':kind,'on':on,'source':source,'ref':ref,'route':route,'outcome':outcome,'homes':homes,'limit':limit,'rule':rule,'confidence':confidence,'by':by,'checked':self.today.isoformat(),'status':'pending' if confidence=='low' else 'confirmed','machine':machine}
        old=self.get('events',k)
        if old and old.get('resolved'):row.update(status=old['status'],resolved=old['resolved'])
        with self.db:self.put('events',k,row)
        return k
    def resolve(self,key,checker,accept):
        e=self.get('events',key)
        if e['status']!='pending':raise ValueError(e['status'])
        if checker==e['by']:raise ValueError('second checker must be someone else')
        e.update(status='confirmed' if accept else 'rejected',resolved={'by':checker,'on':self.today.isoformat()})
        with self.db:self.put('events',key,e)
    def last(self,case_id,points):
        cs=[r for r in self.rows('checks',case_id) if r['point'] in points]
        return max(cs,key=lambda r:r['on']) if cs else None
    def check(self,case_id,point,by,sources):
        c=self.get('cases',case_id)
        if point not in self.CHECKS:raise ValueError(point)
        if not set(sources)<=set(self.SOURCES) or not set(self.REQUIRED)<=set(sources) or any(v not in ('found','none','unavailable') for v in sources.values()):raise ValueError('sources: each required source recorded as found, none or unavailable')
        if point in self.POINTS and self.today<self.months(c['refused'],self.POINTS[point]):raise ValueError('check-point not reached')
        k=f'{case_id}|{point}|{self.today.isoformat()}'
        with self.db:
            self.put('checks',k,{'case_id':case_id,'point':point,'on':self.today.isoformat(),'by':by,'sources':sources})
            for p in ([point] if point in self.POINTS else [p for p,n in self.POINTS.items() if self.today>=self.months(c['refused'],n)] if point=='trace' else []):
                s=self.snapshot(case_id,p)
                old=self.get('snapshots',f'{case_id}|{p}')
                s['history']=(old['history']+[{x:old[x] for x in ('state','homes','by','on')}]) if old else []
                s.update(by=by,on=self.today.isoformat(),second=None)
                self.put('snapshots',f'{case_id}|{p}',s)
        return k
    def state(self,evs,chk,pending):
        rel=[e for e in evs if e['kind'] in ('application','decision','appeal','appeal_decision','householder')]
        won=sorted((e for e in rel if e['kind']=='decision' and e['outcome']=='approved' and e['route'] in self.WON or e['kind']=='appeal_decision' and e['outcome']=='allowed'),key=lambda e:e['on'])
        if won:return 'won',won[-1]
        if pending:return 'pending',None
        refs=lambda k:{e['ref'] for e in rel if e['kind']==k}
        if refs('application')-refs('decision') or refs('appeal')-refs('appeal_decision') or any(e['kind']=='decision' and e['outcome']=='approved' and e['route'] in ('pip','ldc') for e in rel):return 'undecided',None
        dec=[e for e in rel if e['kind'] in ('decision','appeal_decision')]
        if dec and all(e['outcome'] in ('refused','dismissed') for e in dec):return 'refused',None
        if rel and all(e['kind']=='householder' for e in rel):return 'changed',None
        if not rel:return ('none' if chk and all(chk['sources'].get(s) in ('found','none') for s in self.REQUIRED) else 'gap'),None
        return 'other',None
    def delivery(self,evs,won,at):
        if not won:return None
        st=sorted(e['on'] for e in evs if e['kind']=='started')
        comp=[e for e in evs if e['kind']=='completed']
        lapse=self.months(won['on'],12*(won.get('limit') or 3)).isoformat()
        return {'consented':won['on'],'started':st[0] if st else None,'start_sources':sorted({e['source'] for e in evs if e['kind']=='started'}),'completed':min(e['on'] for e in comp) if len({e['source'] for e in comp})>=2 else None,'completion_sources':sorted({e['source'] for e in comp}),'discharged':any(e['kind']=='discharge' for e in evs),'lapse':lapse,'lapsed':not st and at>=lapse}
    def snapshot(self,case_id,point):
        c=self.get('cases',case_id)
        at=(self.months(c['refused'],self.POINTS[point]) if point in self.POINTS else self.today).isoformat()
        chk=self.last(case_id,(point,'trace'))
        evs=[e for e in self.rows('events',case_id) if e['on']<=at]
        ok=[e for e in evs if e['status']=='confirmed']
        state,won=self.state(ok,chk,sum(e['status']=='pending' for e in evs))
        lr=chk['sources'].get('landregistry') if chk else None
        return {'case_id':case_id,'point':point,'at':at,'group':c['group'],'borough':c['borough'],'category':c['category'],'refused':c['refused'],'state':state,'homes':won['homes'] if won else None,'sold':True if any(e['kind']=='sold' for e in ok) else False if lr in ('found','none') else None,'delivery':self.delivery(ok,won,at),'pending':sum(e['status']=='pending' for e in evs),'unavailable':sorted(s for s,v in chk['sources'].items() if v=='unavailable') if chk else None,'checked':chk['on'] if chk else None}
    def due(self):
        out=[]
        for c in self.rows('cases'):
            done={r['point'] for r in self.rows('checks',c['case_id'])}
            if c['group']=='historical':
                if 'trace' not in done:out.append((c['case_id'],'trace'))
                continue
            out+=[(c['case_id'],p) for p,n in self.POINTS.items() if p not in done and self.today>=self.months(c['refused'],n)]
            if c['group']=='supported' and c['tracking'] and c['active']:
                s=self.snapshot(c['case_id'],None)
                d=s['delivery']
                if d and (d['completed'] or d['lapsed']):continue
                k='quarterly' if s['state']=='won' else 'monthly'
                ls=[r['on'] for r in self.rows('checks',c['case_id'])]
                if not ls or self.months(max(ls),3 if k=='quarterly' else 1)<=self.today:out.append((c['case_id'],k))
        return out
    def sample(self,rate=0.1,seed=None):
        seed=seed or self.today.isoformat()
        pool=[s for s in self.rows('snapshots') if s['second'] is None]
        key=lambda s:f"{s['case_id']}|{s['point']}"
        must={key(s) for s in pool if s['group']=='supported' and s['point']=='m12' and s['state']=='none'}
        rest=sorted(key(s) for s in pool if key(s) not in must)
        pick=must|set(random.Random(seed).sample(rest,min(len(rest),math.ceil(len(pool)*rate))))
        with self.db:
            for s in pool:
                s['second']='pending' if key(s) in pick else 'skip'
                self.put('snapshots',key(s),s)
        return sorted(pick)
    def second(self,case_id,point,by,state,homes=None):
        s=self.get('snapshots',f'{case_id}|{point}')
        if s['second']!='pending':raise ValueError(s['second'])
        if by==s['by']:raise ValueError('second checker must be someone else')
        if state not in self.STATES:raise ValueError(state)
        s['second']={'by':by,'state':state,'homes':homes,'agree':state==s['state'] and homes==s['homes'],'on':self.today.isoformat()}
        with self.db:self.put('snapshots',f'{case_id}|{point}',s)
        return s['second']['agree']
    def gaps(self):
        out={}
        for r in self.rows('checks'):
            b=out.setdefault(self.get('cases',r['case_id'])['borough'],{})
            for s,v in r['sources'].items():
                if v=='unavailable':b[s]=b.get(s,0)+1
        return {k:v for k,v in out.items() if v}
    def issues(self,key,issues,by):
        e=self.get('events',key)
        if e['kind']!='appeal_decision':raise ValueError(e['kind'])
        if not issues or any(t not in Coding.THEMES or k not in Coding.CATEGORIES or not isinstance(u,bool) for t,k,u in issues):raise ValueError('issues')
        e['issues']={'items':[{'theme':t,'category':k,'upheld':u} for t,k,u in issues],'by':by,'on':self.today.isoformat()}
        with self.db:self.put('events',key,e)
    def refusals(self):return [{'case_id':e['case_id'],'ref':e['ref'],'on':e['on'],'route':e['route']} for e in self.rows('events') if e['status']=='confirmed' and e['kind']=='decision' and e['outcome']=='refused']
    def appeals(self):return [{'case_id':e['case_id'],'ref':e['ref'],'on':e['on'],'outcome':e['outcome'],'borough':self.get('cases',e['case_id'])['borough'],'issues':e['issues']['items']} for e in self.rows('events') if e['status']=='confirmed' and e['kind']=='appeal_decision' and e.get('issues')]
    def table(self,point):
        out=[]
        for c in self.rows('cases'):
            if not self.last(c['case_id'],(point,'trace')):continue
            s=self.snapshot(c['case_id'],point)
            st=self.get('snapshots',f"{c['case_id']}|{point}") or {}
            s['second']=st.get('second')
            out.append(s)
        return out
    def outcomes(self,point):
        out={}
        for s in self.table(point):
            g=out.setdefault(s['borough'],{}).setdefault(s['category'] or 'uncoded',{})
            g[s['state']]=g.get(s['state'],0)+1
        return out
    def noreturn(self,group,point):
        t=[s['state'] for s in self.table(point) if s['group']==group]
        lo=sum(x in ('none','changed') for x in t)
        return {'n':len(t),'lower':lo/len(t) if t else None,'upper':(lo+sum(x in ('gap','pending','other') for x in t))/len(t) if t else None}
    def score(self,auto):
        cases={c['case_id'] for c in self.rows('cases') if c['group']=='historical' and self.last(c['case_id'],('trace',))}
        hand={(e['case_id'],e['ref']) for e in self.rows('events') if e['case_id'] in cases and e['status']=='confirmed' and e['kind'] in ('application','decision','appeal','appeal_decision')}
        auto={(i,r) for i,r in auto if i in cases}
        hit=len(auto&hand)
        return {'cases':len(cases),'precision':hit/len(auto) if auto else None,'recall':hit/len(hand) if hand else None,'missed':sorted(hand-auto),'false':sorted(auto-hand)}
    def survival(self,out=None):
        rows=[]
        for c in self.rows('cases'):
            sn=[s for s in sorted(self.rows('snapshots',c['case_id']),key=lambda s:self.POINTS.get(s['point'],99)) if s['point'] in self.POINTS and s['state'] not in ('gap','pending')]
            if not sn:continue
            r0=date.fromisoformat(c['refused'])
            hit=next((s for s in sn if s['state'] in ('won','changed')),None)
            row={'case_id':c['case_id'],'group':c['group'],'borough':c['borough']}
            if hit is None:row.update(left=(date.fromisoformat(sn[-1]['at'])-r0).days,right=None,cause=0)
            else:
                i=sn.index(hit)
                row.update(left=(date.fromisoformat(sn[i-1]['at'])-r0).days if i else 0,right=(date.fromisoformat(hit['at'])-r0).days,cause=1 if hit['state']=='won' else 2)
                if hit['state']=='won' and c['group']=='supported' and c['tracking'] and hit.get('delivery'):row['left']=row['right']=(date.fromisoformat(hit['delivery']['consented'])-r0).days
            rows.append(row)
        p=out or env('SURVIVAL_CSV','survival.csv')
        with open(p,'w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=['case_id','group','borough','left','right','cause'])
            w.writeheader()
            w.writerows(rows)
        if not shutil.which('Rscript'):return {'data':p,'rows':len(rows),'model':None,'note':'install R and the intccr package to fit the competing-risks model'}
        s=p+'.R'
        with open(s,'w') as f:f.write(f'library(intccr)\nd<-read.csv("{p}")\nd$right[is.na(d$right)]<-Inf\nd$supported<-as.integer(d$group=="supported")\nfit<-ciregic(Surv2(v=left,u=right,event=cause)~supported,data=d,alpha=c(1,1),nboot=200,do.par=FALSE)\nprint(summary(fit))\n')
        r=subprocess.run(['Rscript',s],capture_output=True,text=True,timeout=3600)
        return {'data':p,'rows':len(rows),'model':r.stdout if r.returncode==0 else None,'error':r.stderr[-2000:] if r.returncode else None}
    def run(self,auto=('register','pins','landregistry','datahub','epc')):
        out,due={'checked':[],'manual':[],'errors':[],'for_people':[]},{}
        for cid,point in self.due():due.setdefault(cid,[]).append(point)
        for cid,points in due.items():
            got,cands={},[]
            for s in auto:
                try:
                    x=self.search(cid,s)
                    cands+=x
                    got[s]=x
                except Exception as e:
                    got[s]=None
                    out['errors'].append((cid,s,str(e)[:200]))
            res=self.link(cid,cands)
            out['for_people']+=[(cid,x) for x in res['for_people']]
            src={s:'unavailable' if v is None else 'found' if any(f'|{s}|' in k for k in res['events']) else 'none' for s,v in got.items()}
            if any('|register|' in k for k in res['events']):src['register']='found'
            for point in points:
                if all(k in src for k in self.REQUIRED):
                    self.check(cid,point,'machine',src)
                    out['checked'].append((cid,point))
            if self.snapshot(cid,None)['state']=='won':out['manual'].append((cid,['voa','buildingcontrol','cil','visit']))
        return out
class Feedback(Tools):
    THRESHOLD=20
    WORKING=5
    REPLY=14
    CYCLE=56
    KINDS=('pack','note','guidance','annual','submission')
    REVIEWS={'pack':('senior','analyst'),'note':('senior',),'guidance':('senior',),'annual':('senior','analyst'),'submission':('senior',)}
    NOTE=('found','propose','effect','cost','measure','range','change')
    EVENTS=('examination','localplan','consultation')
    def __init__(self,store,register,coding,tracking,interviews=None,today=None):
        os.makedirs(store,exist_ok=True)
        self.today=today or date.today()
        ro=lambda p:sqlite3.connect(f'file:{os.path.abspath(p)}?mode=ro',uri=True) if p else None
        self.reg,self.coding,self.tracking,self.interviews=ro(register),ro(coding),ro(tracking),ro(interviews)
        self.db=sqlite3.connect(os.path.join(store,'feedback.db'))
        with self.db:
            for t in ('docs','calendar','measured'):self.db.execute(f'create table if not exists {t}(id text primary key,cid text,data text)')
    def get(self,table,key):
        r=self.db.execute(f'select data from {table} where id=?',(key,)).fetchone()
        return json.loads(r[0]) if r else None
    def put(self,table,key,row):self.db.execute(f'insert or replace into {table} values(?,?,?)',(key,row.get('case_id',''),json.dumps(row,default=str)))
    def rows(self,table):return [json.loads(d) for (d,) in self.db.execute(f'select data from {table} order by id')]
    def save(self,d):
        with self.db:self.put('docs',d['id'],d)
    def read(self,db,table):return [json.loads(d) for (d,) in db.execute(f'select data from {table} order by id')] if db else []
    def quarter(self,d):return f'{str(d)[:4]}Q{(int(str(d)[5:7])-1)//3+1}'
    def back(self,q,n):
        k=int(q[5])-1-n
        return f'{int(q[:4])+k//4}Q{k%4+1}'
    def qend(self,q):
        y,m=int(q[:4]),int(q[5])*3
        return date(y,m,calendar.monthrange(y,m)[1])
    def refused(self):return [{'case_id':r['case_id'],'reference':r.get('reference'),'borough':r.get('borough'),'q':self.quarter(r['decision_date']),'homes':r.get('homes_net') or 0} for r in self.read(self.reg,'register') if r.get('decision_date')]
    def coded(self):
        cs={c['case_id']:c for c in self.read(self.coding,'cases') if c.get('status')=='coded'}
        rs,who={},{}
        for r in self.read(self.coding,'reasons'):
            if r['case_id'] not in cs:continue
            f=r.get('adjudicated') or r
            rs.setdefault(r['case_id'],[]).append({'theme':f['theme'],'category':f['category'],'text':r.get('text')})
            who.setdefault(r['case_id'],set()).update({r.get('coder'),(r.get('adjudicated') or {}).get('by')})
        for r in self.read(self.coding,'second'):who.setdefault(r['case_id'],set()).add(r.get('coder'))
        return [{'case_id':i,'borough':c['facts'].get('borough'),'q':self.quarter(c['facts']['decision_date']),'homes':c['facts'].get('homes_net') or 0,'overturn':bool(c['facts'].get('overturn')),'reasons':rs.get(i,[]),'coders':sorted(who.get(i,set())-{None})} for i,c in cs.items()]
    def boroughs(self):return sorted({x['borough'] for x in self.refused()+self.coded() if x['borough']})
    def appeals(self):
        cases={c['case_id']:c for c in self.read(self.tracking,'cases')}
        return [{'case_id':e['case_id'],'borough':cases[e['case_id']]['borough'],'q':self.quarter(e['on']),'outcome':e['outcome'],'issues':(e.get('issues') or {}).get('items',[])} for e in self.read(self.tracking,'events') if e['kind']=='appeal_decision' and e['status']=='confirmed' and e['outcome'] in ('allowed','dismissed')]
    def stats(self,borough,q,point='m12'):
        share=lambda a,b:a/b if b else None
        cs=[c for c in self.coded() if c['borough']==borough and c['q']==q]
        rs=[r for c in cs for r in c['reasons']]
        top={}
        for r in rs:top[(r['theme'],r['category'])]=top.get((r['theme'],r['category']),0)+1
        ap=[a for a in self.appeals() if a['borough']==borough]
        iss=[i for a in ap for i in a['issues']]
        up={}
        for key in ('category','theme'):
            for k in sorted({i[key] for i in iss}):
                x=[i for i in iss if i[key]==k]
                up.setdefault(key,{})[k]={'n':len(x),'upheld':share(sum(i['upheld'] for i in x),len(x))}
        sn=[s for s in self.read(self.tracking,'snapshots') if s['borough']==borough and s['point']==point]
        ov={c['case_id'] for c in self.coded() if c['borough']==borough and c['overturn']}
        oa=[a for a in ap if a['case_id'] in ov]
        iv=[i for i in self.read(self.interviews,'interviews') if i.get('borough')==borough and isinstance(i.get('understood'),bool)]
        return {'cases':len(cs),'reasons':len(rs),'homes':sum(c['homes'] for c in cs),'mix':{k:share(sum(r['category']==k for r in rs),len(rs)) for k in Coding.CATEGORIES},'top':[{'theme':t,'category':k,'n':n} for (t,k),n in sorted(top.items(),key=lambda x:(-x[1],x[0]))[:5]],'avoidable':share(sum(bool(c['reasons']) and all(r['category'] in ('E','D') for r in c['reasons']) for c in cs),len(cs)),'next':{'n':len(sn),**{s:sum(x['state']==s for x in sn) for s in Tracking.STATES}},'won':share(sum(x['state']=='won' for x in sn),len(sn)),'upheld':{'all':{'n':len(iss),'upheld':share(sum(i['upheld'] for i in iss),len(iss))},'category':up.get('category',{}),'theme':up.get('theme',{})},'overturns':{'n':len(ov),'appealed':len(oa),'allowed':sum(a['outcome']=='allowed' for a in oa)},'understood':{'n':len(iv),'share':share(sum(i['understood'] for i in iv),len(iv))}}
    def pack(self,borough,q,point='m12'):
        al={b:self.stats(b,q,point) for b in self.boroughs()}
        s=al[borough]
        def rng(f):
            xs=sorted(v for v in (f(x) for x in al.values()) if v is not None)
            return {'min':xs[0],'median':statistics.median(xs),'max':xs[-1],'boroughs':len(xs)} if xs else None
        trend=[{'quarter':k,'refusals':len(v),'homes':sum(x['homes'] for x in v)} for k in [self.back(q,n) for n in (3,2,1,0)] for v in [[x for x in self.refused() if x['borough']==borough and x['q']==k]]]
        work=sorted(({'theme':t,**x} for t,x in s['upheld']['theme'].items() if x['n']>=self.WORKING),key=lambda x:(-x['upheld'],x['theme']))
        changes=[{'note':d['id'],'propose':d['body']['propose'],'implemented':d.get('implemented'),'review_on':d.get('review_on'),'result':d.get('result')} for d in self.rows('docs') if d['kind']=='note' and d['borough']==borough and d.get('accepted') and not d.get('retired')]
        return {'borough':borough,'quarter':q,'volume':trend,'mix':{'value':s['mix'],'london':{k:rng(lambda x:x['mix'][k]) for k in Coding.CATEGORIES},'top':s['top']},'avoidable':{'value':s['avoidable'],'cases':s['cases'],'london':rng(lambda x:x['avoidable'])},'next':{'point':point,'value':s['next'],'won':s['won'],'london':rng(lambda x:x['won'])},'appeals':{'value':s['upheld'],'london':rng(lambda x:x['upheld']['all']['upheld'])},'overturns':s['overturns'],'understood':{'value':s['understood'],'london':rng(lambda x:x['understood']['share'])},'working':{'items':work,'note':None if work else f'no topic has {self.WORKING} or more appeal decisions yet'},'changes':changes}
    def draft(self,kind,borough,body,cases=(),q=None,target=None):
        if kind not in self.KINDS:raise ValueError(kind)
        if kind=='note' and (not target or target[0] not in Coding.THEMES or target[1] not in Coding.CATEGORIES):raise ValueError('target: the theme and category the change aims at')
        text=json.dumps(body,default=str).lower()
        if any(x and x.lower() in text for r in self.refused() for x in (r['case_id'],r['reference'])):raise ValueError('names a case')
        if kind in ('pack','annual') and any(b.lower() in text for b in self.boroughs() if b!=borough):raise ValueError('names another borough')
        st=None
        if kind=='note':
            if set(body)!=set(self.NOTE) or not all(isinstance(body[k],str) and body[k].strip() for k in self.NOTE):raise ValueError('found, propose, effect, cost, measure, range and change all stated')
            cs=[c for c in self.coded() if c['case_id'] in set(cases) and (borough is None or c['borough']==borough)]
            if len(cs)!=len(set(cases)):raise ValueError('evidence must be coded cases in that borough')
            qs={}
            for c in cs:qs[c['q']]=qs.get(c['q'],0)+1
            if len(cs)<self.THRESHOLD:raise ValueError(f'fewer than {self.THRESHOLD} cases')
            if len(qs)<2:raise ValueError('pattern must persist across two quarters')
            st={'n':len(cs),'quarters':dict(sorted(qs.items())),'coders':sorted({x for c in cs for x in c['coders']})}
        d={'id':f'{kind}-{len(self.rows("docs"))+1:04d}','kind':kind,'borough':borough,'quarter':q,'body':body,'cases':sorted(set(cases)),'target':{'theme':target[0],'category':target[1]} if target else None,'standards':st,'status':'draft','reviews':[],'drafted':self.today.isoformat(),'response':None}
        self.save(d)
        return d['id']
    def review(self,doc_id,reviewer,role):
        d=self.get('docs',doc_id)
        if d['status']!='draft':raise ValueError(d['status'])
        if role not in self.REVIEWS[d['kind']]:raise ValueError(role)
        if d['kind']=='note' and reviewer in d['standards']['coders']:raise ValueError('reviewer coded some of these cases')
        if any(r['by']==reviewer and r['role']!=role for r in d['reviews']):raise ValueError('each review by a different person')
        d['reviews']=[r for r in d['reviews'] if r['role']!=role]+[{'by':reviewer,'role':role,'on':self.today.isoformat()}]
        if {r['role'] for r in d['reviews']}>=set(self.REVIEWS[d['kind']]):d['status']='reviewed'
        self.save(d)
    def send(self,doc_id):
        d=self.get('docs',doc_id)
        if d['status']!='reviewed':raise ValueError(d['status'])
        if not d['borough']:raise ValueError('London-wide: circulate after review')
        d.update(status='sent',sent=self.today.isoformat())
        self.save(d)
    def reply(self,doc_id,response):
        d=self.get('docs',doc_id)
        if d['status']!='sent':raise ValueError(d['status'])
        d.update(status='replied',response=response,replied=self.today.isoformat())
        self.save(d)
    def circulate(self,doc_id):
        d=self.get('docs',doc_id)
        if d['borough']:
            if d['status'] not in ('sent','replied'):raise ValueError(d['status'])
            if d['status']=='sent' and (self.today-date.fromisoformat(d['sent'])).days<self.REPLY:raise ValueError('right of reply: two weeks')
        elif d['status']!='reviewed':raise ValueError(d['status'])
        d.update(status='circulated',circulated=self.today.isoformat())
        self.save(d)
        return {'body':d['body'],'response':d['response']}
    def decide(self,doc_id,accepted,owner):
        d=self.get('docs',doc_id)
        if d['kind']!='note' or d['status'] not in ('replied','circulated'):raise ValueError('a note the borough has had the chance to answer')
        d.update(accepted=bool(accepted),owner=owner,decided=self.today.isoformat())
        self.save(d)
    def implement(self,doc_id,on,review_on):
        d=self.get('docs',doc_id)
        if not d.get('accepted'):raise ValueError('not accepted')
        if review_on<=on:raise ValueError('review after implementation')
        d.update(implemented=on,review_on=review_on)
        self.save(d)
    def result(self,doc_id,effect,worked):
        d=self.get('docs',doc_id)
        if not d.get('implemented'):raise ValueError('not implemented')
        d['result']={'effect':effect,'worked':bool(worked),'on':self.today.isoformat()}
        if not worked:d['retired']=self.today.isoformat()
        self.save(d)
    def changes(self):return [{k:d.get(k) for k in ('id','borough','drafted','status','accepted','owner','decided','implemented','review_on','result','retired')} for d in self.rows('docs') if d['kind']=='note']
    def calendar(self,name,deadline,kind,borough=None):
        if kind not in self.EVENTS:raise ValueError(kind)
        date.fromisoformat(deadline)
        with self.db:self.put('calendar',name,{'name':name,'deadline':deadline,'kind':kind,'borough':borough})
    def upcoming(self,days=60):return sorted((e for e in self.rows('calendar') if 0<=(date.fromisoformat(e['deadline'])-self.today).days<=days),key=lambda e:e['deadline'])
    def rates(self):
        share=lambda a,b:a/b if b else None
        sn=[s for s in self.read(self.tracking,'snapshots') if s['point']=='m36' and s['group']!='supported']
        ap=self.appeals()
        return {'returns':{'value':share(sum(s['state']=='won' for s in sn),len(sn)),'n':len(sn)},'appeals':{'value':share(sum(a['outcome']=='allowed' for a in ap),len(ap)),'n':len(ap)}}
    def measures(self):
        share=lambda a,b:a/b if b else None
        docs=self.rows('docs')
        packs=[d for d in docs if d['kind']=='pack' and d['quarter']]
        notes=[d for d in docs if d['kind']=='note']
        cs,ap,iv={},{},{}
        for c in self.coded():
            g=cs.setdefault(c['q'],[0,0])
            g[0]+=1
            g[1]+=bool(c['reasons']) and all(r['category'] in ('E','D') for r in c['reasons'])
        for a in self.appeals():
            for i in a['issues']:
                if i['category']=='J':
                    g=ap.setdefault(a['q'],[0,0])
                    g[0]+=1
                    g[1]+=not i['upheld']
        for i in self.read(self.interviews,'interviews'):
            if isinstance(i.get('understood'),bool):
                g=iv.setdefault(self.quarter(i['on']),[0,0])
                g[0]+=1
                g[1]+=i['understood']
        ms={}
        for c in self.coded():
            for r in c['reasons']:
                m=self.cached(r.get('text'))
                if m is not None:
                    g=ms.setdefault(c['q'],[0,0])
                    g[0]+=1
                    g[1]+=m
        trend=lambda x:{q:{'n':n,'share':share(k,n)} for q,(n,k) in sorted(x.items())}
        return {'packs':len(packs),'on_time':share(sum(bool(d.get('circulated')) and date.fromisoformat(d['circulated'])<=self.qend(d['quarter'])+timedelta(days=self.CYCLE) for d in packs),len(packs)),'replied':share(sum(bool(d.get('replied')) for d in packs),len(packs)),'notes':{'proposed':len(notes),'accepted':sum(bool(d.get('accepted')) for d in notes),'implemented':sum(bool(d.get('implemented')) for d in notes),'worked':sum(bool((d.get('result') or {}).get('worked')) for d in notes),'retired':sum(bool(d.get('retired')) for d in notes),'acceptance':share(sum(bool(d.get('accepted')) for d in notes),sum('accepted' in d for d in notes))},'avoidable':trend(cs),'j_overturned':trend(ap),'understood':trend(iv),'measured':trend(ms)}
    UNITS=r'\d+(?:\.\d+)?\s*(?:m²|m2\b|sq\.?\s?m\b|sqm\b|square met(?:re|er)s?|met(?:re|er)s?\b|mm\b|m\b|%|per ?cent|degrees?\b|°|cycle (?:parking )?spaces?\b|car (?:parking )?spaces?\b)'
    def measured(self,text):
        if re.search(self.UNITS,text,re.I):return {'measured':True,'rule':'pattern'}
        a=self.jev().ask(text,{'m':Jev.noul('Does this refusal reason name a specific measured shortfall?','It states a measured figure the scheme fails, such as an area, distance, height, percentage or count','It names a policy or a harm without a measured figure')})['m']
        return {'measured':a['answer'] if a['accepted'] else None,'rule':'jev','p':a.get('p'),'model':a['model']}
    def cached(self,text):
        r=self.get('measured',hashlib.sha256((text or '').encode()).hexdigest())
        return r['measured'] if r else None
    def measure_all(self):
        n=0
        for c in self.coded():
            for r in c['reasons']:
                k=hashlib.sha256((r.get('text') or '').encode()).hexdigest()
                if not r.get('text') or self.get('measured',k):continue
                try:m=self.measured(r['text'])
                except Exception:continue
                if m['measured'] is not None:
                    with self.db:self.put('measured',k,m)
                    n+=1
        return n
    def guidance(self,borough,q=None):
        q=q or self.back(self.quarter(self.today),1)
        qs=[self.back(q,n) for n in range(4)]
        st={k:self.stats(borough,k) for k in qs}
        top={}
        for s in st.values():
            for x in s['top']:top[(x['theme'],x['category'])]=top.get((x['theme'],x['category']),0)+x['n']
        facts={'quarters':qs,'refusals':sum(s['cases'] for s in st.values()),'top_reasons':[{'theme':t,'category':k,'n':n} for (t,k),n in sorted(top.items(),key=lambda x:(-x[1],x[0]))[:5]],'avoidable_share':{k:s['avoidable'] for k,s in st.items()},'upheld_on_appeal_by_topic':st[q]['upheld']['theme']}
        t=self.llm().ask("You write short practical guidance for small builders and agents: what gets refused in this London borough and how to avoid it. Plain English. Use only the figures given. Never name a case, an officer or a councillor. Do not criticise the council.",'Write the guidance: the most common reasons for refusal, what to include in an application to avoid each, and which concerns the council is consistently upheld on at appeal.\n\nFIGURES:\n'+json.dumps(facts,default=str))
        return self.draft('guidance',borough,{'text':t,'figures':facts},q=q)
    def annual(self,year):
        share=lambda a,b:a/b if b else None
        qs=[f'{year}Q{k}' for k in range(1,5)]
        cs=[c for c in self.coded() if c['q'] in qs]
        rs=[r for c in cs for r in c['reasons']]
        iss=[i for a in self.appeals() if a['q'][:4]==str(year) for i in a['issues']]
        ch=self.changes()
        figs={'year':year,'boroughs':len({c['borough'] for c in cs}),'refusals_coded':len(cs),'homes_refused':sum(c['homes'] for c in cs),'reason_mix':{k:share(sum(r['category']==k for r in rs),len(rs)) for k in Coding.CATEGORIES},'avoidable':share(sum(bool(c['reasons']) and all(r['category'] in ('E','D') for r in c['reasons']) for c in cs),len(cs)),'appeal_issues':len(iss),'council_reasons_upheld':share(sum(i['upheld'] for i in iss),len(iss)),'base_rates':self.rates(),'changes':{'proposed':len(ch),'accepted':sum(bool(x['accepted']) for x in ch),'implemented':sum(bool(x['implemented']) for x in ch),'retired':sum(bool(x['retired']) for x in ch)}}
        t=self.llm().ask('You write the annual London report of an independent small-sites unit. Report evidence, not advocacy. London-wide figures only: never name or rank a borough, case, officer or councillor. State sample sizes and uncertainty. Say what is working as well as what is not.','Write the report from these figures.\n\nFIGURES:\n'+json.dumps(figs,default=str))
        return self.draft('annual',None,{'text':t,'figures':figs},q=str(year))
    def submission(self,name):
        e=self.get('calendar',name)
        if not e:raise KeyError(name)
        share=lambda a,b:a/b if b else None
        q=self.back(self.quarter(self.today),1)
        qs=[self.back(q,n) for n in range(4)]
        cs=[c for c in self.coded() if c['q'] in qs]
        top,by={},{}
        for c in cs:
            g=by.setdefault(c['borough'],[0,0])
            g[0]+=1
            g[1]+=bool(c['reasons']) and all(r['category'] in ('E','D') for r in c['reasons'])
            for r in c['reasons']:top[(r['theme'],r['category'])]=top.get((r['theme'],r['category']),0)+1
        av=sorted(k/n for n,k in by.values() if n)
        iss=[i for a in self.appeals() for i in a['issues']]
        figs={'quarters':qs,'refusals':len(cs),'top_reasons':[{'theme':t,'category':k,'n':n} for (t,k),n in sorted(top.items(),key=lambda x:(-x[1],x[0]))[:10]],'avoidable_range_between_boroughs':{'min':av[0],'median':statistics.median(av),'max':av[-1],'boroughs':len(av)} if av else None,'appeal_issues':len(iss),'council_reasons_upheld':share(sum(i['upheld'] for i in iss),len(iss))}
        t=self.llm().ask('You draft evidence submissions on small-sites policy for a planning examination or local plan review. Report evidence and its uncertainty; do not campaign for more approvals; respect deliberate policy choices. Never name a case, officer or councillor, and never rank boroughs.',f"Draft the submission for: {name} (deadline {e['deadline']}). Use only these figures and state the sample sizes.\n\nFIGURES:\n"+json.dumps(figs,default=str))
        return self.draft('submission',e.get('borough'),{'text':t,'figures':figs,'for':name,'deadline':e['deadline']})
    def run(self):
        q=self.back(self.quarter(self.today),1)
        have={(d['borough'],d['quarter']) for d in self.rows('docs') if d['kind']=='pack'}
        made,errors=[],[]
        for b in self.boroughs():
            if (b,q) in have:continue
            try:made.append(self.draft('pack',b,self.pack(b,q),q=q))
            except Exception as e:errors.append((b,str(e)[:200]))
        return {'quarter':q,'packs':made,'errors':errors,'measured':self.measure_all(),'upcoming':self.upcoming()}
class Evaluation(Tools):
    REGIMES={'resubmission':('2023-12-06',True,'end of the free resubmission'),'appeals':('2026-04-01',True,'appeals decided on the material the council had, for applications dated from this day'),'delegation':('2026-10-31',False,'national scheme of delegation, date unconfirmed')}
    TARGETS={'stage1_wrongly_excluded':('<',0.02,1),'stage1_critical_errors':('<',0.02,1),'stage4_first_contact':('>=',0.9,4),'stage4_interviews':('>=',0.6,4)}
    DEPENDS={'Q1':(1,4,5,6),'Q2':(3,5,6),'Q3':(1,2,7),'Q4':(1,4,5,6),'Q5':(1,6),'Q7':(4,5,6)}
    HARMS=('complaint','enforcement','advocacy','other')
    BACK=('won','undecided','refused','other')
    ZERO='0'*64
    def __init__(self,store,today=None,**paths):
        os.makedirs(store,exist_ok=True)
        self.today=today or date.today()
        self.paths,self.src={k:v for k,v in paths.items() if v},{}
        self.db=sqlite3.connect(os.path.join(store,'evaluation.db'))
        with self.db:self.db.execute('create table if not exists ledger(seq integer primary key autoincrement,prev text,hash text,body text)')
    def now(self):return datetime.now().isoformat(timespec='seconds')
    def read(self,name,table):
        if name not in self.src and os.path.exists(self.paths.get(name) or ''):self.src[name]=sqlite3.connect(f'file:{os.path.abspath(self.paths[name])}?mode=ro',uri=True)
        return [json.loads(d) for (d,) in self.src[name].execute(f'select data from {table} order by id')] if name in self.src else []
    def quarter(self,d):return f'{str(d)[:4]}Q{(int(str(d)[5:7])-1)//3+1}'
    def back(self,q,n):
        k=int(q[5])-1-n
        return f'{int(q[:4])+k//4}Q{k%4+1}'
    def append(self,kind,data):
        last=self.db.execute('select hash from ledger order by seq desc limit 1').fetchone()
        prev=last[0] if last else self.ZERO
        body=json.dumps({'kind':kind,'on':self.now(),'data':data},sort_keys=True,default=str)
        h=hashlib.sha256((prev+body).encode()).hexdigest()
        with self.db:cur=self.db.execute('insert into ledger(prev,hash,body) values(?,?,?)',(prev,h,body))
        return cur.lastrowid,h
    def verify(self):
        prev=self.ZERO
        for seq,p,h,body in self.db.execute('select seq,prev,hash,body from ledger order by seq'):
            if p!=prev or hashlib.sha256((prev+body).encode()).hexdigest()!=h:return seq
            prev=h
        return None
    def entries(self,kind=None):return [{'seq':s,'hash':h,**json.loads(b)} for s,h,b in self.db.execute('select seq,hash,body from ledger order by seq') if kind is None or json.loads(b)['kind']==kind]
    def plan(self,primary,comparisons,method,rules,evaluators,start,targets=None,base=None,success=None):
        if self.entries('plan'):raise ValueError('plan already published: amend it')
        if self.today.isoformat()>=start:raise ValueError('publish before phase 2 starts')
        if not all((primary,comparisons,method,evaluators)) or not {'cost_per_home','skill_min'}<=set(rules):raise ValueError('primary outcome, comparisons, method, decision rules and evaluators')
        return self.append('plan',{'primary':primary,'comparisons':comparisons,'method':method,'rules':rules,'evaluators':sorted(evaluators),'start':start,'targets':targets or {},'base':base,'success':success})[1]
    def amend(self,field,value,why):
        p=self.locked()
        if field not in p or not why:raise ValueError('an existing field, with the reason')
        self.append('amendment',{'field':field,'value':value,'why':why,'after_start':self.today.isoformat()>=p['start']})
    def locked(self):
        p=self.entries('plan')
        if not p:raise ValueError('no published plan: no analysis before pre-registration')
        out={**p[0]['data'],'hash':p[0]['hash']}
        for a in self.entries('amendment'):out[a['data']['field']]=a['data']['value']
        return out
    def result(self,question,data):
        data={**data,'question':question,'plan':self.locked()['hash']}
        data['seq']=self.append('result',data)[0]
        return data
    def size(self,p0,p1,alpha=0.05,power=0.8):
        z=statistics.NormalDist()
        pb=(p0+p1)/2
        return math.ceil((z.inv_cdf(1-alpha/2)*math.sqrt(2*pb*(1-pb))+z.inv_cdf(power)*math.sqrt(p0*(1-p0)+p1*(1-p1)))**2/(p1-p0)**2)
    def deff(self,n,m,icc):return math.ceil(n*(1+(m-1)*icc))
    def order(self,boroughs,starts,seed):
        p=self.locked()
        if self.entries('order'):raise ValueError('rollout order already drawn')
        if len(boroughs)!=len(starts):raise ValueError('one start date per borough')
        bs=sorted(boroughs)
        random.Random(hashlib.sha256((seed+p['hash']).encode()).hexdigest()).shuffle(bs)
        m=dict(zip(bs,sorted(starts)))
        self.append('order',{'seed':seed,'starts':m})
        return m
    def exposed(self,borough,d):
        o=self.entries('order')
        s=o[0]['data']['starts'].get(borough) if o else None
        return bool(s and str(d)>=s)
    def wedge(self,point='m12',perms=5000,seed='wedge'):
        self.locked()
        o=self.entries('order')
        if not o:raise ValueError('no rollout order drawn')
        st=o[0]['data']['starts']
        rows=[{'borough':s['borough'],'period':self.quarter(s['refused']),'refused':s['refused'],'y':int(s['state']=='won')} for s in self.snaps(point).values() if s['state'] not in ('gap','pending') and s['group']!='historical']
        exp=lambda m:[int(r['borough'] in m and r['refused']>=m[r['borough']]) for r in rows]
        def stat(e):
            t=w=0
            for q in sorted({r['period'] for r in rows}):
                a=[r['y'] for r,x in zip(rows,e) if r['period']==q and x]
                b=[r['y'] for r,x in zip(rows,e) if r['period']==q and not x]
                if a and b:
                    t+=(len(a)+len(b))*(sum(a)/len(a)-sum(b)/len(b))
                    w+=len(a)+len(b)
            return t/w if w else None
        e0=exp(st)
        obs=stat(e0)
        if obs is None:raise ValueError('no period has both exposed and unexposed refusals yet')
        bs=sorted(st)
        ds=[st[b] for b in bs]
        pool=[list(x) for x in itertools.permutations(ds)] if len(ds)<=8 else [random.Random(f'{seed}{i}').sample(ds,len(ds)) for i in range(perms)]
        vals=[v for v in (stat(exp(dict(zip(bs,x)))) for x in pool) if v is not None]
        g=None
        try:
            import pandas,statsmodels.api as sm,statsmodels.formula.api as smf
            f=smf.gee('y ~ exposed + C(period)',groups='borough',data=pandas.DataFrame(rows).assign(exposed=e0),family=sm.families.Binomial(),cov_struct=sm.cov_struct.Exchangeable()).fit(cov_type='bias_reduced')
            g={'log_odds':float(f.params['exposed']),'ci':[float(x) for x in f.conf_int().loc['exposed']],'p':float(f.pvalues['exposed'])}
        except Exception as ex:g={'error':str(ex)[:300]}
        return self.result('Q1',{'design':'wedge','point':point,'n':len(rows),'boroughs':len({r['borough'] for r in rows}),'risk_difference':obs,'p_randomisation':sum(abs(v)>=abs(obs)-1e-12 for v in vals)/len(vals),'orders':len(vals),'smallest_p':1/len(vals),'gee':g})
    def enter(self):
        self.locked()
        seen={i for e in self.entries('entrants') for i in e['data']['cases']}
        new=sorted(r['case_id'] for r in self.read('handoff','handoff') if r['case_id'] not in seen)
        if not new:return None
        n=len(self.entries('entrants'))+1
        self.append('entrants',{'round':n,'cases':new})
        return n
    def draw(self,n,capacity,seed):
        e=next(x for x in self.entries('entrants') if x['data']['round']==n)
        if any(x['data']['round']==n for x in self.entries('lottery')):raise ValueError('round already drawn')
        cs=e['data']['cases']
        win=sorted(random.Random(hashlib.sha256((seed+e['hash']).encode()).hexdigest()).sample(cs,min(capacity,len(cs))))
        self.append('lottery',{'round':n,'capacity':capacity,'seed':seed,'winners':win,'losers':sorted(set(cs)-set(win))})
        return win
    def snaps(self,point):return {s['case_id']:s for s in self.read('tracking','snapshots') if s['point']==point}
    def diff(self,a,b):
        n1,n0=len(a),len(b)
        if not n1 or not n0:return {'n':[n1,n0],'p':None,'diff':None,'ci':None,'p_value':None}
        z=statistics.NormalDist()
        p1,p0=sum(a)/n1,sum(b)/n0
        se=math.sqrt(p1*(1-p1)/n1+p0*(1-p0)/n0)
        pb=(sum(a)+sum(b))/(n1+n0)
        sp=math.sqrt(pb*(1-pb)*(1/n1+1/n0))
        return {'n':[n1,n0],'p':[p1,p0],'diff':p1-p0,'ci':[p1-p0-1.96*se,p1-p0+1.96*se],'p_value':2*(1-z.cdf(abs(p1-p0)/sp)) if sp else None}
    def effect(self,point='m12'):
        self.locked()
        win={i for e in self.entries('lottery') for i in e['data']['winners']}
        lose={i for e in self.entries('lottery') for i in e['data']['losers']}
        sn=self.snaps(point)
        known=lambda ids:[sn[i] for i in sorted(ids) if i in sn and sn[i]['state'] not in ('gap','pending')]
        a,b=known(win),known(lose)
        r=self.diff([s['state']=='won' for s in a],[s['state']=='won' for s in b])
        homes=lambda x:sum(s['homes'] or 0 for s in x)/len(x) if x else None
        r.update(design='lottery',point=point,arms={'winners':len(win),'losers':len(lose)},missing={'winners':len(win)-len(a),'losers':len(lose)-len(b)},homes={'winners':homes(a),'losers':homes(b)})
        return self.result('Q1',r)
    def months(self,d,n):
        d=date.fromisoformat(d) if isinstance(d,str) else d
        y,m=d.year+(d.month+n-1)//12,(d.month+n-1)%12+1
        return date(y,m,min(d.day,calendar.monthrange(y,m)[1])).isoformat()
    def action(self,e):
        if e['kind'] in ('appeal','appeal_decision'):return 'appeal'
        if e['kind'] in ('application','decision'):return 'alternative' if e.get('route') in ('prior','pip','tdc','ldc') else 'application'
        return None
    def outcomes(self,window=36):
        acts=self.locked().get('success')
        if not acts:raise ValueError('the published plan must say, for each route, which action counts as acting on it')
        ev,out={},[]
        for e in self.read('tracking','events'):
            if e['status']=='confirmed':ev.setdefault(e['case_id'],[]).append(e)
        for c in self.read('panel','cases'):
            d=c.get('decision')
            if not d or not c['facts'].get('decision_date'):continue
            end=self.months(c['facts']['decision_date'],window)
            es=sorted((e for e in ev.get(c['case_id'],[]) if e['on']<=end),key=lambda e:e['on'])
            for route in sorted({d['route'],d['alt']}-{None,'stop'}):
                a=acts.get(route)
                acted=[e for e in es if self.action(e)==a]
                if not acted:continue
                later=[e for e in es if e['on']>=acted[0]['on']]
                won=any(e['kind']=='appeal_decision' and e['outcome']=='allowed' for e in later) if a=='appeal' else any(e['kind']=='decision' and e['outcome']=='approved' and e['route'] in Tracking.WON or e['kind']=='appeal_decision' and e['outcome']=='allowed' for e in later)
                out.append((c['case_id'],route,won))
        return out
    def score(self,xs,base):
        if not xs:return None
        p=lambda b:Panel.MID[Panel.BANDS.index(b)]
        n=len(xs)
        ob=sum(y for _,y in xs)/n
        bs=sum((p(b)-y)**2 for b,y in xs)/n
        ref=sum((base-y)**2 for _,y in xs)/n
        tab={b:{'n':len(k),'predicted':p(b),'observed':sum(k)/len(k)} for b in Panel.BANDS if (k:=[y for bb,y in xs if bb==b])}
        return {'n':n,'brier':bs,'base':base,'skill':1-bs/ref if ref else None,'reliability':sum(t['n']*(t['predicted']-t['observed'])**2 for t in tab.values())/n,'resolution':sum(t['n']*(t['observed']-ob)**2 for t in tab.values())/n,'uncertainty':ob*(1-ob),'table':tab}
    def calibration(self,outcomes):
        pl=self.locked()
        base=pl['base'] if pl.get('base') is not None else Panel.BASE['returns'][0]
        o={(i,r):bool(s) for i,r,s in outcomes}
        p=lambda b:Panel.MID[Panel.BANDS.index(b)]
        ratings=self.read('panel','ratings')
        agreed,members,pairs=[],{},[]
        for c in self.read('panel','cases'):
            d=c.get('decision')
            if not d:continue
            rs=[r for r in ratings if r['case_id']==c['case_id']]
            for route,band in ((d['route'],d['band']),(d['alt'],d['altband'])):
                if band is None or (c['case_id'],route) not in o:continue
                y=o[(c['case_id'],route)]
                agreed.append((route,band,y))
                own=[]
                for r in rs:
                    for rr,bb in ((r['route'],r['band']),(r['alt'],r['altband'])):
                        if rr==route and bb:
                            members.setdefault(r['member'],[]).append((bb,y))
                            own.append(p(bb))
                if own:pairs.append((sum(own)/len(own),p(band),y))
        brier=lambda xs:sum((q-y)**2 for q,y in xs)/len(xs) if xs else None
        return self.result('Q2',{'all':self.score([(b,y) for _,b,y in agreed],base),'routes':{r:self.score([(b,y) for rr,b,y in agreed if rr==r],base) for r in sorted({r for r,_,_ in agreed})},'members':{m:self.score(x,base) for m,x in sorted(members.items())},'private_mean':brier([(a,y) for a,_,y in pairs]),'agreed':brier([(b,y) for _,b,y in pairs]),'stop_testable':'only on historical cases where the applicant went ahead'})
    def coded(self):
        cs={c['case_id']:c for c in self.read('coding','cases') if c.get('status')=='coded'}
        rs={}
        for r in self.read('coding','reasons'):
            if r['case_id'] in cs:rs.setdefault(r['case_id'],[]).append(r.get('adjudicated') or r)
        return [{'case_id':i,'borough':c['facts'].get('borough'),'q':self.quarter(c['facts']['decision_date']),'reasons':rs.get(i,[])} for i,c in cs.items()]
    def did(self,note_id,window=2,comparison=None,applications=None):
        self.locked()
        d=next(x for x in self.read('feedback','docs') if x['id']==note_id)
        if not d.get('implemented') or not d.get('target'):raise ValueError('an implemented change note with a target')
        t,q0=d['target'],self.quarter(d['implemented'])
        pre,post=sorted(self.back(q0,k) for k in range(1,window+1)),sorted(self.back(q0,-k) for k in range(window))
        cs=self.coded()
        same={x['borough'] for x in self.read('feedback','docs') if x['kind']=='note' and x.get('implemented') and x.get('target')==t}
        comp=sorted(comparison or {c['borough'] for c in cs}-same)
        g=lambda bs,qs:[any(r['theme']==t['theme'] and r['category']==t['category'] for r in c['reasons']) for c in cs if c['borough'] in bs and c['q'] in qs]
        x={'treated_before':g({d['borough']},pre),'treated_after':g({d['borough']},post),'comparison_before':g(set(comp),pre),'comparison_after':g(set(comp),post)}
        n={k:len(v) for k,v in x.items()}
        pr={k:sum(v)/len(v) for k,v in x.items() if v}
        est=se=None
        if len(pr)==4:
            est=(pr['treated_after']-pr['treated_before'])-(pr['comparison_after']-pr['comparison_before'])
            se=math.sqrt(sum(pr[k]*(1-pr[k])/n[k] for k in pr))
        apps={k:sum(applications.get((b,q),0) for b in bs for q in qs) for k,(bs,qs) in {'treated_before':({d['borough']},pre),'treated_after':({d['borough']},post)}.items()} if applications else None
        return self.result('Q3',{'note':note_id,'target':t,'borough':d['borough'],'comparison':comp,'before':pre,'after':post,'n':n,'shares':pr,'did':est,'ci':[est-1.96*se,est+1.96*se] if est is not None else None,'applications':apps})
    def its(self,note_id,lags=1):
        self.locked()
        d=next(x for x in self.read('feedback','docs') if x['id']==note_id)
        if not d.get('implemented') or not d.get('target'):raise ValueError('an implemented change note with a target')
        t,q0=d['target'],self.quarter(d['implemented'])
        by={}
        for c in self.coded():
            if c['borough']==d['borough']:
                g=by.setdefault(c['q'],[0,0])
                g[0]+=1
                g[1]+=any(r['theme']==t['theme'] and r['category']==t['category'] for r in c['reasons'])
        qs=sorted(by)
        o=lambda q:int(q[:4])*4+int(q[5])-1
        k=o(q0)-o(qs[0]) if qs else 0
        if sum(q<q0 for q in qs)<2 or sum(q>=q0 for q in qs)<2:raise ValueError('need at least two quarters either side of the change')
        import numpy,statsmodels.api as sm
        y=numpy.array([by[q][1]/by[q][0] for q in qs])
        tt=numpy.array([o(q)-o(qs[0]) for q in qs],dtype=float)
        post=(tt>=k).astype(float)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            f=sm.OLS(y,sm.add_constant(numpy.column_stack([tt,post,(tt-k)*post]))).fit(cov_type='HAC',cov_kwds={'maxlags':lags})
        ci=f.conf_int()
        return self.result('Q3',{'design':'its','note':note_id,'target':t,'borough':d['borough'],'series':{q:{'n':by[q][0],'share':by[q][1]/by[q][0]} for q in qs},'level_change':float(f.params[2]),'level_ci':[float(x) for x in ci[2]],'slope_change':float(f.params[3]),'slope_ci':[float(x) for x in ci[3]],'short_series':len(qs)<8})
    def cost(self,effect,people,homes,staff,overheads,basis='consented',prevention=0,benchmark=None):
        self.locked()
        calloff=sum(x['cost'] for c in self.read('support','cases') for x in c['spend'] if x['payer']=='unit')
        total=staff+overheads+calloff
        per=lambda e:total/(e*people*homes) if e is not None and e>0 else None
        lo,hi=effect['ci'] if effect['ci'] else (None,None)
        return self.result('Q4',{'basis':basis,'effect':effect['seq'],'total':total,'calloff':calloff,'optimistic':per(hi),'central':per(effect['diff']),'pessimistic':per(lo),'prevention_homes':prevention,'benchmark':benchmark})
    def resubmissions(self):return [(s['refused'],s['state'] in self.BACK) for s in self.snaps('m12').values() if s['group']!='supported' and s['state'] not in ('gap','pending')]
    def around(self,regime,rows,windows=(90,180,365)):
        self.locked()
        cut=date.fromisoformat(self.REGIMES[regime][0])
        out={}
        for w in windows:
            out[str(w)]=self.diff([y for d,y in rows if cut<=date.fromisoformat(d)<cut+timedelta(days=w)],[y for d,y in rows if cut-timedelta(days=w)<=date.fromisoformat(d)<cut])
        return self.result('Q5',{'regime':regime,'cutoff':cut.isoformat(),'confirmed':self.REGIMES[regime][1],'windows':out})
    def quality(self,values):
        p=self.locked()
        t={**self.TARGETS,**{k:tuple(v) for k,v in p['targets'].items()}}
        ok=lambda op,x,th:{'<':x<th,'<=':x<=th,'>':x>th,'>=':x>=th}[op]
        checks={k:{'value':values.get(k),'target':f'{op} {th}','stage':s,'pass':None if values.get(k) is None else ok(op,values[k],th)} for k,(op,th,s) in sorted(t.items())}
        bad={c['stage'] for c in checks.values() if c['pass'] is not True}
        return self.result('Q6',{'checks':checks,'warnings':{q:sorted(bad&set(st)) for q,st in self.DEPENDS.items() if bad&set(st)}})
    def breakdown(self,groups,point='m12'):
        self.locked()
        sn=self.snaps(point)
        out={}
        for i,label in groups.items():
            if i in sn and sn[i]['state'] not in ('gap','pending'):
                g=out.setdefault(str(label),[0,0])
                g[0]+=1
                g[1]+=sn[i]['state']=='won'
        return self.result('Q7',{'point':point,'groups':{k:{'n':n,'won':w/n} for k,(n,w) in sorted(out.items())}})
    def imd(self,uprn):
        lsoa,dec=env('ONSUD_LSOA','lsoa21cd'),env('IOD_DECILE','Index of Multiple Deprivation (IMD) Decile (where 1 is most deprived 10% of LSOAs)')
        if '_ons' not in self.__dict__:
            self._ons=Lookup(env('ONSUD'),env('ONSUD_UPRN','uprn'),(lsoa,))
            self._iod=Lookup(env('IOD_FILE'),env('IOD_LSOA','LSOA code (2021)'),(dec,))
        r=self._ons.get(uprn)
        if not r:return None
        i=self._iod.get(r[lsoa.lower()])
        v=i[dec.lower()] if i else None
        return {'lsoa':r[lsoa.lower()],'decile':int(float(v)) if v not in (None,'') else None}
    NDSS={(1,1,1):39,(1,2,1):50,(1,2,2):58,(2,3,1):61,(2,3,2):70,(2,4,1):70,(2,4,2):79,(3,4,1):74,(3,4,2):84,(3,4,3):90,(3,5,1):86,(3,5,2):93,(3,5,3):99,(3,6,1):95,(3,6,2):102,(3,6,3):108,(4,5,1):90,(4,5,2):97,(4,5,3):103,(4,6,1):99,(4,6,2):106,(4,6,3):112,(4,7,1):108,(4,7,2):115,(4,7,3):121,(4,8,1):117,(4,8,2):124,(4,8,3):130,(5,6,1):103,(5,6,2):110,(5,6,3):116,(5,7,1):112,(5,7,2):119,(5,7,3):125,(5,8,1):121,(5,8,2):128,(5,8,3):134,(6,7,1):116,(6,7,2):123,(6,7,3):129,(6,8,1):125,(6,8,2):132,(6,8,3):138}
    def homes(self,files):
        self.locked()
        out={}
        for cid,path in files.items():
            t=Pdf.text(path)
            d=self.llm().data('You read area schedules for new homes and copy the figures exactly.','Return JSON {"homes":[{"name":"as written","bedrooms":integer,"bedspaces":integer,"storeys":integer,"area":number in square metres}]}. Use null where the schedule does not say. Count a studio as 1 bedroom and 1 bedspace.\n\nSCHEDULE:\n'+t[:100000])
            rows=[]
            for h in d.get('homes') or []:
                need,a=self.NDSS.get((h.get('bedrooms'),h.get('bedspaces'),h.get('storeys') or 1)),h.get('area')
                seen=a is not None and re.search(r'(?<![\d.])'+re.escape(f'{float(a):g}')+r'(?!\d)',t) is not None
                rows.append({**h,'standard':need,'verified':seen,'meets':None if need is None or not seen else float(a)>=need})
            out[cid]=rows
        known=[r['meets'] for v in out.values() for r in v if r['meets'] is not None]
        return self.result('Q7',{'homes_quality':{'cases':len(out),'homes':sum(len(v) for v in out.values()),'checked':len(known),'meet_space_standard':sum(known)/len(known) if known else None,'detail':out,'daylight':'not assessed here: needs the specialist reports'}})
    def neighbours(self):return [{'case_id':c['case_id'],'changed':r['changed'],'risk':r['risk']} for c in self.read('support','cases') if c.get('status')=='consented' for r in c['matrix'] if r['code'].startswith('amenity/')]
    def harm(self,kind,note):
        if kind not in self.HARMS or not note:raise ValueError(kind)
        self.append('harm',{'kind':kind,'note':note})
    def drift(self):
        out={}
        for c in self.read('panel','cases'):
            d=c.get('decision')
            if d:
                g=out.setdefault(self.quarter(d['on']),[0,0])
                g[0]+=1
                g[1]+=d['route']=='stop'
        return self.result('Q7',{'stop_share':{q:{'n':n,'stop':s/n} for q,(n,s) in sorted(out.items())}})
    def rules(self,effect,cost,skill,prevention=None,casework=None):
        r=self.locked()['rules']
        harms=len(self.entries('harm'))
        acts=[]
        if effect['ci'] and effect['ci'][0]>0 and cost['central'] is not None and cost['central']<r['cost_per_home'] and not harms:acts.append('scale up')
        if skill is None or skill<r['skill_min']:acts.append('redesign the panel')
        if prevention is not None and casework is not None and prevention>casework:acts.append('shift money from casework to prevention')
        return self.result('rules',{'effect':effect['seq'],'cost':cost['seq'],'skill':skill,'harms':harms,'actions':acts or ['continue as planned']})
    def signoff(self,seq,evaluator):
        if evaluator not in self.locked()['evaluators']:raise ValueError('independent evaluator named in the plan')
        if not any(e['seq']==seq for e in self.entries('result')):raise ValueError('no such result')
        self.append('signoff',{'result':seq,'by':evaluator})
    def report(self):
        signed={}
        for e in self.entries('signoff'):signed.setdefault(e['data']['result'],[]).append(e['data']['by'])
        return {'intact':self.verify() is None,'plan':self.entries('plan'),'amendments':self.entries('amendment'),'order':self.entries('order'),'lotteries':self.entries('lottery'),'results':[{**e['data'],'signed':signed.get(e['seq'],[])} for e in self.entries('result')],'harms':self.entries('harm')}
    def run(self):
        p,out=self.locked(),{}
        jobs=[('drift',self.drift)]
        if self.entries('lottery'):jobs.append(('Q1_lottery',self.effect))
        if self.entries('order'):jobs.append(('Q1_wedge',self.wedge))
        if p.get('success'):jobs.append(('Q2',lambda:self.calibration(self.outcomes())))
        jobs.append(('Q5_resubmission',lambda:self.around('resubmission',self.resubmissions())))
        for k,f in jobs:
            try:out[k]=f()['seq']
            except Exception as e:out[k]=f'not run: {str(e)[:200]}'
        out['intact']=self.verify() is None
        return out
