"""Small educational Lambda pipeline. All SQL sessions use UTC."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import time
from urllib.error import HTTPError
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import psycopg
from psycopg.types.json import Jsonb

EARLIEST_EVENT_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)
ALLOWED_LATENESS = timedelta(seconds=120)

def connect():
    return psycopg.connect(os.environ['DATABASE_URL'], options='-c timezone=UTC')

def validate(p):
    if not isinstance(p, dict) or set(p) != {'event_id', 'sensor_id', 'event_time', 'value'}:
        raise ValueError('required fields: event_id, sensor_id, event_time, value')
    for field in ('event_id', 'sensor_id'):
        if not isinstance(p[field], str) or not 1 <= len(p[field]) <= 100:
            raise ValueError('invalid ' + field)
    if type(p['value']) is not int or not -1000000 <= p['value'] <= 1000000:
        raise ValueError('value must be integer in [-1000000,1000000]')
    if not isinstance(p['event_time'], str):
        raise ValueError('event_time must be ISO8601')
    dt = datetime.fromisoformat(p['event_time'].replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('event_time must include timezone')
    dt = dt.astimezone(timezone.utc)
    if dt < EARLIEST_EVENT_TIME:
        raise ValueError('event_time is before 1970-01-01 UTC')
    if dt > datetime.now(timezone.utc) + timedelta(minutes=5):
        raise ValueError('event_time is over 5 minutes in future')
    return p['event_id'], p['sensor_id'], dt, p['value']

def process_one(crash=False):
    with connect() as c:
        # Serialize state + canonical insertion: also safe with multiple consumers.
        c.execute('SELECT pg_advisory_xact_lock(2202)')
        row = c.execute('SELECT arrival_id,payload FROM raw_events WHERE NOT processed ORDER BY arrival_id LIMIT 1 FOR UPDATE').fetchone()
        if not row:
            return False
        aid, payload = row
        try:
            eid, sid, dt, val = validate(payload)
        except (ValueError, TypeError, OverflowError) as ex:
            c.execute('INSERT INTO quarantine VALUES(%s,%s)', (aid, str(ex)))
        else:
            existing = c.execute('SELECT sensor_id,event_time,value FROM events WHERE event_id=%s', (eid,)).fetchone()
            if existing:
                if existing != (sid, dt, val):
                    c.execute('INSERT INTO quarantine VALUES(%s,%s)', (aid, 'conflicting payload for event_id=' + eid))
            else:
                maximum = c.execute('SELECT max_event_time FROM stream_state WHERE singleton').fetchone()[0]
                late = maximum is not None and maximum - dt > ALLOWED_LATENESS
                c.execute('INSERT INTO events VALUES(%s,%s,%s,%s,%s,%s)', (eid, sid, dt, val, aid, late))
                c.execute('UPDATE stream_state SET max_event_time=GREATEST(max_event_time,%s)', (dt,))
                c.execute("""INSERT INTO minute_metrics VALUES(date_trunc('minute',%s::timestamptz),%s,1,%s)
                  ON CONFLICT(bucket,sensor_id) DO UPDATE SET
                  event_count=minute_metrics.event_count+1,value_sum=minute_metrics.value_sum+EXCLUDED.value_sum""", (dt, sid, val))
        c.execute('UPDATE raw_events SET processed=true WHERE arrival_id=%s', (aid,))
        if crash:
            print('FAULT INJECTION: exiting before COMMIT', flush=True)
            os._exit(77)
    return True

def batch():
    with connect() as c:
        c.execute('SELECT pg_advisory_xact_lock(2203)')
        c.execute('DELETE FROM daily_metrics')
        c.execute("""INSERT INTO daily_metrics
          SELECT (event_time AT TIME ZONE 'UTC')::date,sensor_id,count(*),sum(value),now()
          FROM events GROUP BY 1,2""")
    print('BATCH committed', flush=True)

def snapshot():
    with connect() as c:
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        result = {}
        for table in ('raw_events', 'events', 'quarantine'):
            result[table] = c.execute('SELECT count(*) FROM ' + table).fetchone()[0]
        result['pending'] = c.execute('SELECT count(*) FROM raw_events WHERE NOT processed').fetchone()[0]
        result['late'] = c.execute('SELECT count(*) FROM events WHERE is_late').fetchone()[0]
        # Clamp the display for state written by versions that accepted year 1.
        result['watermark'] = c.execute("""
            SELECT CASE WHEN max_event_time IS NOT NULL THEN GREATEST(
                max_event_time - %s, TIMESTAMPTZ '0001-01-01 00:00:00+00') END
            FROM stream_state
        """, (ALLOWED_LATENESS,)).fetchone()[0]
        result['minute_metrics'] = c.execute('SELECT * FROM minute_metrics ORDER BY 1,2').fetchall()
        result['daily_metrics'] = c.execute('SELECT day,sensor_id,event_count,value_sum FROM daily_metrics ORDER BY 1,2').fetchall()
        return result

def reject_non_json_number(token):
    raise ValueError(f'{token} is not a JSON number')


class Handler(BaseHTTPRequestHandler):
    def send_json(self, status, data):
        body = json.dumps(data, default=str, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != '/events':
            return self.send_json(404, {'error': 'not found'})
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 65536:
                return self.send_json(413, {'error': 'body size must be 1..65536 bytes'})
            payload = json.loads(self.rfile.read(size), parse_constant=reject_non_json_number)
        except (ValueError, RecursionError):
            return self.send_json(400, {'error': 'invalid JSON'})
        try:
            with connect() as c:
                aid = c.execute('INSERT INTO raw_events(payload) VALUES(%s) RETURNING arrival_id', (Jsonb(payload),)).fetchone()[0]
        except (psycopg.DataError, RecursionError):
            return self.send_json(422, {'error': 'JSON cannot be stored as jsonb'})
        except psycopg.Error:
            return self.send_json(503, {'error': 'storage unavailable; retry same event_id'})
        # Response only after durable database commit.
        self.send_json(202, {'arrival_id': aid})

    def do_GET(self):
        if self.path == '/health':
            with connect() as c:
                c.execute('SELECT 1')
            self.send_json(200, {'status': 'ok'})
        elif self.path in ('/', '/metrics'):
            self.send_json(200, snapshot())
        else:
            self.send_json(404, {'error': 'not found'})

BASE = datetime(2026, 11, 24, 12, tzinfo=timezone.utc)
def event(i, seconds, value):
    return dict(event_id='demo-' + str(i), sensor_id='sensor-01', event_time=(BASE + timedelta(seconds=seconds)).isoformat(), value=value)

def fixture():
    return [event(1,0,10), event(2,60,20), event(3,300,30), event(2,60,20),
            event(4,30,40), event(5,-86400,50), event(6,240,60),
            event(1,0,999), {'event_id': 'broken'}]

def ingest_url(path):
    return os.environ.get('INGEST_URL', 'http://localhost:18082') + path


def post(body):
    return urllib.request.Request(ingest_url('/events'), body, {'Content-Type': 'application/json'})


def send(payload):
    req = post(json.dumps(payload).encode())
    with urllib.request.urlopen(req, timeout=10) as r:
        assert r.status == 202
        return json.load(r)

def wait_drained():
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if snapshot()['pending'] == 0:
            return
        time.sleep(.2)
    raise AssertionError('stream did not drain in 40 seconds')

def wait_daily(expected_rows):
    deadline = time.monotonic() + 40
    last_rows = []
    while time.monotonic() < deadline:
        with urllib.request.urlopen(ingest_url('/metrics'), timeout=5) as response:
            last_rows = json.load(response)['daily_metrics']
        if last_rows == expected_rows:
            return
        time.sleep(.2)
    raise AssertionError(f'Background batch did not publish {expected_rows}; saw {last_rows}')


def check_rejected_json():
    for body, expected in ((b'{"value":NaN}', 400), (br'{"event_id":"\u0000"}', 422)):
        try:
            with urllib.request.urlopen(post(body), timeout=10) as response:
                raise AssertionError(f'Expected HTTP {expected}, got {response.status}')
        except HTTPError as response:
            with response:
                assert response.code == expected, response.read()
    assert snapshot()['raw_events'] == 0
    print('PASS invalid JSON: NaN returns 400, NUL returns 422, neither enters the journal')

def check(count, total, late):
    s = snapshot()
    assert s['events'] == count, s
    assert s['late'] == late, s
    assert sum(r[2] for r in s['minute_metrics']) == count, s
    assert sum(r[3] for r in s['minute_metrics']) == total, s
    assert sum(r[2] for r in s['daily_metrics']) == count, s
    assert sum(r[3] for r in s['daily_metrics']) == total, s
    with connect() as c:
        expected_minute = c.execute("SELECT date_trunc('minute',event_time),sensor_id,count(*),sum(value) FROM events GROUP BY 1,2 ORDER BY 1,2").fetchall()
        expected_daily = c.execute("SELECT event_time::date,sensor_id,count(*),sum(value) FROM events GROUP BY 1,2 ORDER BY 1,2").fetchall()
    assert s['minute_metrics'] == expected_minute, s
    assert s['daily_metrics'] == expected_daily, s
    print(json.dumps(s, default=str, ensure_ascii=False, indent=2))

def verify():
    assert snapshot()['raw_events'] == 0, 'Use a new Compose project/empty volume for verify'
    check_rejected_json()
    early = event('early', 0, 1)
    early['event_time'] = '0001-01-01T00:00:00+00:00'
    early_id = send(early)['arrival_id']
    for p in fixture():
        send(p)
    wait_drained()
    with connect() as c:
        reason = c.execute('SELECT reason FROM quarantine WHERE arrival_id=%s', (early_id,)).fetchone()
    assert reason == ('event_time is before 1970-01-01 UTC',), reason
    print('PASS boundary date: year 1 quarantined and the stream drained')
    prior_day = (BASE - timedelta(days=1)).date().isoformat()
    main_day = BASE.date().isoformat()
    wait_daily([[prior_day, 'sensor-01', 1, 50], [main_day, 'sensor-01', 5, 160]])
    check(6,210,2)
    assert snapshot()['quarantine'] == 3
    print('PASS initial: duplicates, disorder, late, conflict, invalid, UTC daily grouping')
    for p in fixture():
        send(p)
    wait_drained()
    check(6,210,2)
    replay = snapshot()
    assert replay['raw_events'] == 19 and replay['quarantine'] == 5, replay
    print('PASS full input replay: aggregates unchanged')
    send(event(8,-86400 + 15,70))
    wait_drained()
    wait_daily([[prior_day, 'sensor-01', 2, 120], [main_day, 'sensor-01', 5, 160]])
    check(7,280,3)
    print('PASS automatic batch: /metrics shows the correction for the older UTC day')
    batch()
    check(7,280,3)
    print('PASS repeated batch: aggregates unchanged')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['serve','stream','batch','batch-loop','verify','snapshot','seed-crash','check-recovery'])
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--crash-before-commit', action='store_true')
    args = parser.parse_args()
    if args.mode == 'serve':
        ThreadingHTTPServer(('0.0.0.0',8080), Handler).serve_forever()
    elif args.mode == 'stream':
        while True:
            done = process_one(args.crash_before_commit)
            if not done:
                if args.once:
                    break
                time.sleep(.2)
    elif args.mode == 'batch':
        batch()
    elif args.mode == 'batch-loop':
        while True:
            batch()
            time.sleep(10)
    elif args.mode == 'verify':
        verify()
    elif args.mode == 'seed-crash':
        print(send(event(9,360,80)))
    elif args.mode == 'check-recovery':
        wait_drained()
        batch()
        check(8,360,3)
        print('PASS crash recovery: one new event, one aggregate increment')
    else:
        print(json.dumps(snapshot(), default=str, ensure_ascii=False, indent=2))
