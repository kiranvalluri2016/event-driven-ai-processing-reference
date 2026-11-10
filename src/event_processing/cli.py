from __future__ import annotations
import argparse,asyncio,json,uuid
from .core import Event,Queue,Worker
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',default='.local/events.db');s=p.add_subparsers(dest='cmd',required=True);e=s.add_parser('enqueue');e.add_argument('tenant');e.add_argument('event');e.add_argument('--mode',default='ok');e.add_argument('--ticket',default='T-1');w=s.add_parser('work');w.add_argument('--limit',type=int,default=10);i=s.add_parser('inspect');i.add_argument('tenant');r=s.add_parser('replay');r.add_argument('tenant');r.add_argument('event');a=p.parse_args();q=Queue(a.db)
 if a.cmd=='enqueue':q.enqueue(Event(a.tenant,a.event,str(uuid.uuid4()),{'mode':a.mode,'ticket_id':a.ticket}));print('ENQUEUED')
 if a.cmd=='work':print(asyncio.run(Worker(q).drain(a.limit)))
 if a.cmd=='inspect':print(json.dumps(q.results(a.tenant)))
 if a.cmd=='replay':q.replay(a.tenant,a.event);print('REPLAYED')
if __name__=='__main__':main()
