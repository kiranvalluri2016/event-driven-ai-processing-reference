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
