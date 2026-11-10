from __future__ import annotations
import asyncio,json,sqlite3,uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC,datetime,timedelta
from pathlib import Path

def now(): return datetime.now(UTC)
def iso(value=None): return (value or now()).isoformat()
@dataclass(frozen=True)
class Event:
 tenant_id:str;event_id:str;correlation_id:str;payload:dict;attempts:int=0;claim_token:str|None=None
class Queue:
 def __init__(self,path:str|Path):
  Path(path).parent.mkdir(parents=True,exist_ok=True);self.path=str(path);c=self._c();c.executescript('''CREATE TABLE IF NOT EXISTS events (tenant TEXT,event_id TEXT,correlation TEXT,payload TEXT,status TEXT,attempts INTEGER,available_at TEXT,last_error TEXT,owner TEXT,lease_until TEXT,claim_token TEXT,PRIMARY KEY(tenant,event_id));CREATE TABLE IF NOT EXISTS results (tenant TEXT,event_id TEXT,result TEXT,PRIMARY KEY(tenant,event_id));CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY AUTOINCREMENT,tenant TEXT,event_id TEXT,correlation TEXT,event TEXT,detail TEXT,at TEXT);''');c.commit();c.close()
 def _c(self):
  c=sqlite3.connect(self.path);c.row_factory=sqlite3.Row;return c
 def _audit(self,c,e,event,detail={}): c.execute('INSERT INTO audit(tenant,event_id,correlation,event,detail,at) VALUES(?,?,?,?,?,?)',(e.tenant_id,e.event_id,e.correlation_id,event,json.dumps(detail,sort_keys=True),iso()))
 def enqueue(self,e:Event):
  c=self._c();c.execute('INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?)',(e.tenant_id,e.event_id,e.correlation_id,json.dumps(e.payload,sort_keys=True),'PENDING',0,iso(),None,None,None,None));self._audit(c,e,'ENQUEUED');c.commit();c.close()
 def claim(self,worker_id='worker',lease_seconds=30,max_attempts=3):
  c=self._c();c.execute('BEGIN IMMEDIATE');c.execute("UPDATE events SET status=CASE WHEN attempts>=? THEN 'DLQ' ELSE 'RETRY' END,owner=NULL,lease_until=NULL WHERE status='PROCESSING' AND lease_until<?",(max_attempts,iso()));row=c.execute("SELECT * FROM events WHERE status IN ('PENDING','RETRY') AND available_at<=? ORDER BY available_at LIMIT 1",(iso(),)).fetchone()
  if not row:c.commit();c.close();return None
  attempts=row['attempts']+1;token=str(uuid.uuid4());c.execute("UPDATE events SET status='PROCESSING',attempts=?,owner=?,lease_until=?,claim_token=? WHERE tenant=? AND event_id=?",(attempts,worker_id,iso(now()+timedelta(seconds=lease_seconds)),token,row['tenant'],row['event_id']));c.commit();c.close();return Event(row['tenant'],row['event_id'],row['correlation'],json.loads(row['payload']),attempts,token)
 def complete(self,e,result):
  c=self._c();c.execute('BEGIN IMMEDIATE');row=c.execute("SELECT status,claim_token FROM events WHERE tenant=? AND event_id=?",(e.tenant_id,e.event_id)).fetchone()
  if not row or row['status']!='PROCESSING' or row['claim_token']!=e.claim_token:c.rollback();c.close();raise RuntimeError('stale claim')
  existing=c.execute('SELECT 1 FROM results WHERE tenant=? AND event_id=?',(e.tenant_id,e.event_id)).fetchone();c.execute('INSERT OR IGNORE INTO results VALUES(?,?,?)',(e.tenant_id,e.event_id,json.dumps(result,sort_keys=True)));c.execute("UPDATE events SET status='COMPLETED' WHERE tenant=? AND event_id=?",(e.tenant_id,e.event_id));self._audit(c,e,'IDEMPOTENT_SKIP' if existing else 'COMPLETED');c.commit();c.close()
 def fail(self,e,max_attempts=3):
  c=self._c();row=c.execute('SELECT attempts,status,claim_token FROM events WHERE tenant=? AND event_id=?',(e.tenant_id,e.event_id)).fetchone()
  if not row or row['status']!='PROCESSING' or row['claim_token']!=e.claim_token:c.close();raise RuntimeError('stale claim')
  attempt=row['attempts'];status='DLQ' if attempt>=max_attempts else 'RETRY';at=iso(now()+timedelta(seconds=2**attempt));c.execute('UPDATE events SET status=?,available_at=?,last_error=? WHERE tenant=? AND event_id=?',(status,at,'synthetic processor failure',e.tenant_id,e.event_id));self._audit(c,e,'DEAD_LETTER' if status=='DLQ' else 'RETRY_SCHEDULED',{'attempt':attempt});c.commit();c.close()
 def replay(self,tenant,event_id):
  c=self._c();row=c.execute('SELECT * FROM events WHERE tenant=? AND event_id=? AND status="DLQ"',(tenant,event_id)).fetchone()
  if not row:c.close();raise ValueError('only tenant-scoped DLQ events are replayable')
  c.execute("UPDATE events SET status='PENDING',attempts=0,available_at=? WHERE tenant=? AND event_id=?",(iso(),tenant,event_id));self._audit(c,Event(tenant,event_id,row['correlation'],json.loads(row['payload'])),'REPLAYED');c.commit();c.close()
 def state(self,tenant,event_id):
  c=self._c();r=c.execute('SELECT status,attempts FROM events WHERE tenant=? AND event_id=?',(tenant,event_id)).fetchone();c.close();return dict(r) if r else None
 def results(self,tenant):
  c=self._c();r=[dict(x) for x in c.execute('SELECT * FROM results WHERE tenant=?',(tenant,))];c.close();return r
class Processor:
 def process(self,e):
  if e.payload.get('mode')=='poison':raise RuntimeError('synthetic poison event')
  if e.payload.get('mode')=='transient' and e.attempts==1: raise RuntimeError('synthetic transient failure')
  return {'classification':'SUPPORT_TICKET','summary':f"ticket:{e.payload.get('ticket_id','unknown')}"}
class Worker:
 def __init__(self,queue,processor=None,max_attempts=3):self.queue=queue;self.processor=processor or Processor();self.max_attempts=max_attempts
 async def run_once(self):
  e=self.queue.claim('sequential-worker',max_attempts=self.max_attempts)
  if not e:return False
  try:self.queue.complete(e,self.processor.process(e))
  except Exception:self.queue.fail(e,self.max_attempts)
  return True
 async def drain(self,limit=20):
  completed=0
  for _ in range(limit):
   if not await self.run_once(): break
   completed+=1
  return completed
