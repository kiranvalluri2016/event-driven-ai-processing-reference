import asyncio
from event_processing.core import Event,Queue,Worker
def ev(tenant='tenant-a',ident='e1',mode='ok'):return Event(tenant,ident,'corr-1',{'ticket_id':'T-1','mode':mode})
def test_at_least_once_idempotent_result_and_tenant_isolation(tmp_path):
 q=Queue(tmp_path/'q.db');q.enqueue(ev());q.enqueue(ev());asyncio.run(Worker(q).drain())
 assert len(q.results('tenant-a'))==1 and q.results('tenant-b')==[] and q.state('tenant-a','e1')['status']=='COMPLETED'
def test_retry_dead_letter_and_controlled_replay(tmp_path):
 q=Queue(tmp_path/'q.db');q.enqueue(ev(mode='poison'));w=Worker(q,max_attempts=2)
 asyncio.run(w.drain());c=q._c();c.execute("UPDATE events SET available_at='2000-01-01T00:00:00+00:00'");c.commit();c.close()
 asyncio.run(w.drain());assert q.state('tenant-a','e1')['status']=='DLQ'
 q.replay('tenant-a','e1');assert q.state('tenant-a','e1')['status']=='PENDING'
def test_restart_recovery_and_tenant_scoped_replay(tmp_path):
 db=tmp_path/'q.db';Queue(db).enqueue(ev());assert asyncio.run(Worker(Queue(db)).drain())==1
 import pytest
 with pytest.raises(ValueError):Queue(db).replay('tenant-b','e1')
def test_lease_expiry_recovers_claim_and_transient_succeeds(tmp_path):
 q=Queue(tmp_path/'q.db');q.enqueue(ev(mode='transient'));first=q.claim('crashed',lease_seconds=-1);assert first.attempts==1
 assert asyncio.run(Worker(q).drain())==1 and q.state('tenant-a','e1')['status']=='COMPLETED'
def test_duplicate_claim_and_trusted_cli_tenant(tmp_path):
 q=Queue(tmp_path/'q.db');q.enqueue(ev());assert q.claim('one');assert q.claim('two') is None
 from event_processing.cli import trusted_tenant
 import pytest
 with pytest.raises(PermissionError):trusted_tenant('tenant-b')
def test_claim_fencing_and_lease_expiry_exhaustion(tmp_path):
 import pytest
 q=Queue(tmp_path/'q.db');q.enqueue(ev());a=q.claim('a',lease_seconds=-1,max_attempts=2);b=q.claim('b',max_attempts=2)
 assert a.claim_token!=b.claim_token
 with pytest.raises(RuntimeError):q.complete(a,{'bad':'stale'})
 q.complete(b,{'ok':True});assert q.state('tenant-a','e1')['status']=='COMPLETED'
 q.enqueue(ev(ident='poison',mode='poison'));first=q.claim('a',lease_seconds=-1,max_attempts=2);second=q.claim('b',lease_seconds=-1,max_attempts=2);assert first and second
 assert q.claim('c',max_attempts=2) is None and q.state('tenant-a','poison')['status']=='DLQ'
