"""Validate forked storage readiness and local-synchronization acknowledgements."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import queue
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import uuid
import zenoh

ROOT = Path(__file__).resolve().parents[2]
TARGET = Path(sys.argv[1])


def wait_for(predicate, message, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate(): return
        time.sleep(0.1)
    raise AssertionError(message)


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Router:
    def __init__(self, root, name, required_failure=False):
        self.root = root / name
        self.root.mkdir()
        self.passwords = {user: secrets.token_hex(24) for user in ['reader', 'writer', 'admin']}
        dictionary = self.root / 'credentials'
        dictionary.write_text(''.join(f'{u}:{p}\n' for u,p in self.passwords.items()))
        dictionary.chmod(0o600)
        self.port = free_port()
        self.endpoint = f'tcp/127.0.0.1:{self.port}'
        self.zid = uuid.uuid4().hex.lstrip('0')
        read_messages = ['query','reply','declare_subscriber']
        rules = [
            {'id':'event-read', 'permission':'allow','flows':['egress'],'messages':['put','delete'],'key_exprs':['test/**','volatile/**','disabled/**']},
            {'id':'read', 'permission':'allow','flows':['ingress','egress'],'messages':read_messages,'key_exprs':['test/**','volatile/**','disabled/**']},
            {'id':'command','permission':'allow','flows':['ingress','egress'],'messages':['query','reply'],'key_exprs':['@storage/**']},
            {'id':'admin','permission':'allow','flows':['ingress','egress'],'messages':['put','delete','query','reply','declare_subscriber','declare_queryable'],'key_exprs':['**','@/**','@storage/**']},
        ]
        self.config = {
            'id':self.zid, 'mode':'router','listen':{'endpoints':[self.endpoint]},
            'scouting':{'multicast':{'enabled':False},'gossip':{'enabled':False}},
            'timestamping':{'enabled':True}, 'plugins_loading':{'enabled':True},
            'adminspace':{'enabled':True,'permissions':{'read':True,'write':True}},
            'transport':{'auth':{'usrpwd':{'dictionary_file':str(dictionary)}}},
            'access_control':{'enabled':True,'default_permission':'deny','rules':rules,
                'subjects':[{'id':user,'usernames':[user]} for user in self.passwords],
                'policies':[{'subjects':['reader'],'rules':['read','event-read']},{'subjects':['writer'],'rules':['read','event-read','command']},{'subjects':['admin'],'rules':['admin']}]},
            'plugins':{'storage_manager':{'__required__':True,'__path__':str(TARGET/'libzenoh_plugin_storage_manager.so'),
                'volumes':{'rocksdb':{'__path__':str(TARGET/'libzenoh_backend_rocksdb.so')}},
                'storages':{
                    'durable':{'required':True,'acknowledged_writes':True,'key_expr':'test/**','volume':{'id':'rocksdb','dir':'state','create_db':True}},
                    'volatile':{'required':True,'acknowledged_writes':True,'key_expr':'volatile/**','volume':'memory'},
                    'disabled':{'key_expr':'disabled/**','volume':'memory'},
                    'bad':{'required':required_failure,'key_expr':'bad/**','volume':{'id':'rocksdb','dir':'missing','create_db':False}},
                }}}
        }
        self.process = None
        self.log = None

    def start(self, expect_exit=False):
        path = self.root/'router.json'
        path.write_text(json.dumps(self.config))
        path.chmod(0o600)
        self.log = (self.root/'router.log').open('a')
        env = dict(os.environ, RUST_LOG='info', NO_COLOR='1', ZENOH_BACKEND_ROCKSDB_ROOT=str(self.root/'db'))
        self.process = subprocess.Popen([str(TARGET/'zenohd'),'-c',str(path)],stdout=self.log,stderr=subprocess.STDOUT,env=env)
        if expect_exit:
            self.process.wait(timeout=15)
            assert self.process.returncode != 0
            return
        def ready():
            if self.process.poll() is not None:
                raise AssertionError('Router exited unexpectedly')
            try:
                with socket.create_connection(('127.0.0.1',self.port),timeout=0.1): return True
            except OSError: return False
        wait_for(ready,'router listener')

    def stop(self, crash=False):
        if self.process and self.process.poll() is None:
            self.process.kill() if crash else self.process.terminate()
            try: self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill(); self.process.wait(timeout=5)
        if self.log: self.log.close()

    def client(self, user):
        return zenoh.open(zenoh.Config.from_json5(json.dumps({'mode':'client','connect':{'endpoints':[self.endpoint],'timeout_ms':3000},
            'scouting':{'multicast':{'enabled':False}},'transport':{'auth':{'usrpwd':{'user':user,'password':self.passwords[user]}}}})))

    def command(self, storage='durable'):
        return f'@storage/{self.zid}/router/{storage}/write'

    def status(self, session):
        key=f'@/{self.zid}/router/status/plugins/storage_manager/readiness'
        values = read(session,key)
        return json.loads(values[key]) if key in values else None


def read(session, key):
    results={}
    for reply in session.get(key,timeout=0.7,target=zenoh.QueryTarget.ALL):
        if reply.ok is not None: results[str(reply.ok.key_expr)]=reply.ok.payload.to_string()
    return results


def write(session, endpoint, key, value=None, operation='put', durability='local_sync', metadata=None):
    request=metadata or {'protocol':1,'operation_id':uuid.uuid4().hex,'operation':operation,'key':key,'durability':durability}
    arguments={'attachment':json.dumps(request),'timeout':1.0,'target':zenoh.QueryTarget.ALL}
    if value is not None: arguments['payload']=value
    replies=list(session.get(endpoint,**arguments))
    if not replies: return None
    assert len(replies)==1, 'Ambiguous responders'
    reply=replies[0]
    if reply.err is not None: return {'rejected':json.loads(reply.err.payload.to_string())}
    ack=json.loads(reply.ok.payload.to_string())
    assert str(reply.ok.key_expr)==endpoint
    assert ack['operation_id']==request['operation_id'] and ack['key']==key
    assert ack['service']==endpoint and ack['durability']==durability
    return ack


def main():
    output=ROOT/'.local/storage-improvements'
    output.mkdir(parents=True,exist_ok=True)
    result={'started_at':datetime.now(timezone.utc).isoformat(),'checks':[],'limits':[
        'SIGKILL recovery does not simulate a power cut or faulty storage hardware',
        'Local synchronization is not replica quorum or exactly-once execution',
        'Operation IDs correlate replies; no deduplication is provided',
        'Readiness reflects initialized workers and observed failures, not continuous disk probes']}
    def passed(name):
        result['checks'].append({'name':name,'status':'passed'})
        print('PASS '+name,flush=True)
    routers=[]
    with tempfile.TemporaryDirectory(prefix='zenoh-fork-proof-') as tmp:
        root=Path(tmp); root.chmod(0o700)
        try:
            r=Router(root,'normal'); routers.append(r); r.start()
            with r.client('admin') as admin:
                wait_for(lambda: (status:=r.status(admin)) is not None and status['ready'], 'required storage readiness')
                status=r.status(admin)
                assert not status['healthy'] and status['storages']['bad']['state']=='failed'
                assert status['storages']['bad']['error']
                assert status['storages']['durable']['state']=='ready'
                passed('optional_backend_failure_is_visible_required_stores_are_ready')
            with r.client('writer') as writer, r.client('reader') as reader:
                assert write(reader,r.command(),'test/forbidden','forbidden') is None
                assert read(reader,'test/forbidden')=={}
                passed('read_only_query_permissions_cannot_invoke_write_command')
                assert write(writer,r.command('disabled'),'disabled/key','value',durability='processed') is None
                assert read(reader,'disabled/key')=={}
                passed('acknowledged_writes_are_disabled_by_default')
                rejected=write(writer,r.command('volatile'),'volatile/key','value')
                assert rejected['rejected']['may_have_applied'] is False
                assert read(reader,'volatile/key')=={}
                passed('unsupported_local_sync_is_rejected_before_mutation')
                assert write(writer,r.command('volatile'),'volatile/key','value',durability='processed')['result']=='inserted'
                assert read(reader,'volatile/key')=={'volatile/key':'value'}
                passed('processed_mode_remains_available_for_volatile_backend')
                assert write(writer,r.command(),'other/key','value')['rejected']['may_have_applied'] is False
                assert write(writer,r.command(),'test/**','value')['rejected']['may_have_applied'] is False
                passed('out_of_scope_and_wildcard_commands_are_rejected')
                events=queue.Queue()
                with reader.declare_subscriber('test/key', lambda sample: events.put(sample.payload.to_string())):
                    ack=write(writer,r.command(),'test/key','first')
                    assert ack['result']=='inserted'
                    assert events.get(timeout=3)=='first'
                passed('acknowledged_write_notifies_live_subscribers')
                ack=write(writer,r.command(),'test/key','after-sync')
                assert ack['result']=='replaced'
                r.stop(crash=True)
            r.start()
            with r.client('reader') as reader:
                wait_for(lambda: read(reader,'test/key')=={'test/key':'after-sync'},'local-sync put recovery')
            passed('local_sync_put_survives_sigkill_without_pre_crash_readback')
            with r.client('writer') as writer:
                ack=write(writer,r.command(),'test/key',operation='delete')
                assert ack['result']=='deleted'
                r.stop(crash=True)
            r.start()
            with r.client('admin') as admin:
                wait_for(lambda: (status:=r.status(admin)) is not None and status['storages']['durable']['state']=='ready','storage restart readiness')
            with r.client('reader') as reader: assert read(reader,'test/key')=={}
            passed('local_sync_delete_survives_sigkill')
            r.stop()
            required=Router(root,'required-failure',required_failure=True)
            routers.append(required); required.start(expect_exit=True)
            assert 'Required storage' in (required.root/'router.log').read_text()
            passed('required_backend_failure_aborts_startup')
            result['status']='passed'
        except Exception as error:
            result['status']='failed'; result['error']=str(error)
            raise
        finally:
            for router in routers:
                router.stop()
                log=router.root/'router.log'
                if log.exists():
                    content='\n'.join(line for line in log.read_text(errors='replace').splitlines() if 'Initial conf:' not in line)
                    for password in router.passwords.values(): content=content.replace(password,'[redacted]')
                    (output/f'{router.root.name}.log').write_text(content+'\n')
            result['finished_at']=datetime.now(timezone.utc).isoformat()
            (output/'proof.json').write_text(json.dumps(result,indent=2)+'\n')
            print('Evidence: '+str(output/'proof.json'),flush=True)

if __name__=='__main__':
    zenoh.init_log_from_env_or('error')
    main()
