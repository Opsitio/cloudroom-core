"""Real Core HTTP scheduling with a deterministic harness; no credentials or inference."""
import json
import unittest
from datetime import datetime, timedelta, timezone

import core_fixture
from core_e2e import until


class SchedulerTests(unittest.TestCase):
    setUp = core_fixture.ReplayTests.setUp
    tearDown = core_fixture.ReplayTests.tearDown
    start = core_fixture.ReplayTests.start

    def prepare(self):
        self.start()
        self.service.request('POST', '/v1/sessions', {'request_id': 'scheduler'}, 202)
        until(lambda: self.service.session('cr_scheduler')['state'] == 'idle', 'session ready')

    def call(self, action, expected=202, **fields):
        return self.service.request('POST', '/v1/automations', {'action': action, **fields}, expected)

    def create(self, **fields):
        return self.call('create', session='cr_scheduler', thread='thr_fixture', name='Test loop',
                         prompt=fields.pop('prompt', 'hello'), **fields)['automation']

    def row(self):
        return self.call('list')['automations'][0]

    def test_one_time_runs_without_app_and_survives_restart(self):
        self.prepare()
        row = self.create(delay='2 seconds')
        self.service.stop()
        self.start()
        until(lambda: self.row()['last_run_state'] == 'completed', 'one-time completed', 25)
        row = self.row()
        self.assertFalse(row['enabled'])
        self.assertEqual(row['runs'], 1)
        request = row['last_request_id']
        self.service.stop()
        self.start()
        self.assertEqual(self.row()['last_request_id'], request)
        self.assertEqual(self.row()['runs'], 1)
        self.assertEqual(self.service.session('cr_scheduler')['receipts'][request]['state'], 'completed')
        self.call('resume', id=row['id'], expected=400)
        self.call('delete', id=row['id'])
        self.assertEqual(self.call('list')['automations'], [])

    def test_manual_run_pause_and_busy_thread_do_not_overlap(self):
        self.prepare()
        row = self.create(cron='* * * * *', enabled=False)
        self.service.request('POST', '/v1/sessions/cr_scheduler/prompts', {'request_id': 'held', 'text': 'hold'}, 202)
        until(lambda: self.service.session('cr_scheduler')['state'] == 'running', 'busy')
        self.call('run', id=row['id'])
        self.call('run', id=row['id'], expected=409)
        self.assertEqual(self.row()['runs'], 1)
        self.call('pause', id=row['id'])
        self.service.request('POST', '/v1/sessions/cr_scheduler/interrupt', {'request_id': 'interrupt', 'target_request_id': 'held'}, 202)
        until(lambda: self.service.session('cr_scheduler')['state'] == 'idle', 'interrupted')
        self.call('run', id=row['id'])
        until(lambda: self.row()['last_run_state'] == 'completed', 'manual run', 20)
        self.assertFalse(self.row()['enabled'])

    def test_unknown_run_is_not_replayed_after_restart(self):
        self.prepare()
        row = self.create(cron='* * * * *', prompt='no-reply')
        self.call('run', id=row['id'])
        until(lambda: (self.repo / 'no-reply').exists(), 'running loop', 15)
        request = self.row()['last_request_id']
        self.service.stop(crash=True)
        self.start()
        until(lambda: self.row()['last_run_state'] in ('unknown_after_restart', 'interrupted'), 'uncertain run settled', 20)
        self.assertEqual(self.row()['last_request_id'], request)
        self.assertEqual(self.row()['runs'], 1)
        if self.row()['last_run_state'] == 'unknown_after_restart':
            self.assertFalse(self.row()['enabled'])

    def test_invalid_requests_and_unsupported_modes_leave_no_loops(self):
        self.prepare()
        self.service.request('POST', '/v1/automations', {'action': 'list'}, 401, token=None)
        base = dict(session='cr_scheduler', name='Check', prompt='hello', cron='* * * * *')
        for changes in [dict(cron='bad'), dict(timezone='Not/AZone'), dict(new_thread=True),
                        dict(script='echo go'), dict(prompt=''), dict(session='missing'),
                        dict(run_at='2020-01-01T00:00:00Z'), dict(typo=True)]:
            self.call('create', expected=400, **(base | changes))
        self.assertEqual(self.call('list')['automations'], [])
        self.assertTrue(self.service.request('GET', '/v1/capabilities')['automations'])

    def test_archived_pauses_and_update_switches_schedule(self):
        self.prepare()
        row = self.create(cron='0 9 * * 1-5', timezone='Europe/Zurich')
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        changed = self.call('update', id=row['id'], cron=None, run_at=future)['automation']
        self.assertIsNone(changed['cron'])
        self.call('archived', session='cr_scheduler')
        self.assertFalse(self.row()['enabled'])
        self.assertIn('archived', self.row()['last_error'])
        self.assertEqual(self.call('list', session='another')['automations'], [])
        stored = self.state / 'automations.json'
        self.assertEqual(stored.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(stored.read_text())['version'], 1)


if __name__ == '__main__':
    unittest.main()
