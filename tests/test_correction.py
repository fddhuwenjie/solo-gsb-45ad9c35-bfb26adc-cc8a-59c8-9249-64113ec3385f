"""处置结论更正 API 测试：替代关系、幂等回放、联单约束与历史快照、并发争用。"""

import json
import os
import sqlite3
import tempfile
import threading
import unittest

_TMP_DB = os.path.join(
    tempfile.mkdtemp(prefix="coldchain-correct-"), "correction.db"
)
os.environ["COLDCHAIN_DB"] = _TMP_DB

from fastapi.testclient import TestClient  # noqa: E402

from app import db  # noqa: E402
from app.main import app  # noqa: E402


def iso(epoch_seconds: int) -> str:
    from datetime import datetime, timezone

    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def make_payload(batch_id="C-001", device_id="reefer-01"):
    """两条异常：0–240（装货段）、1200–1260（运输段）。"""
    return {
        "batch_id": batch_id,
        "device_id": device_id,
        "gap_seconds": 600,
        "dip_tolerance_seconds": 120,
        "samples": [
            {"timestamp": iso(0), "temperature": 9.0},
            {"timestamp": iso(60), "temperature": 9.5},
            {"timestamp": iso(120), "temperature": 5.0},
            {"timestamp": iso(180), "temperature": 10.0},
            {"timestamp": iso(240), "temperature": 11.0},
            {"timestamp": iso(1200), "temperature": 9.0},
            {"timestamp": iso(1260), "temperature": 9.0},
        ],
        "segments": [
            {"segment_id": "seg-1", "segment_type": "loading",
             "start": iso(0), "end": iso(600),
             "lower_limit": 0.0, "upper_limit": 8.0},
            {"segment_id": "seg-2", "segment_type": "transport",
             "start": iso(600), "end": iso(1800),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ],
    }


def make_normal_payload(batch_id="C-ZERO"):
    """无异常批次：空 decisions 即可闭环处置与更正。"""
    return {
        "batch_id": batch_id,
        "device_id": "reefer-01",
        "samples": [
            {"timestamp": iso(0), "temperature": 5.0},
            {"timestamp": iso(60), "temperature": 6.0},
        ],
        "segments": [
            {"segment_id": "seg-1", "segment_type": "loading",
             "start": iso(0), "end": iso(600),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ],
    }


def decisions_closed():
    return [
        {"seq": 0, "decision": "resolved",
         "handled_at": iso(300), "note": "现场已复冷"},
        {"seq": 1, "decision": "dismissed",
         "handled_at": iso(1300), "note": "传感器故障，误报"},
    ]


def decisions_corrected():
    """更正后的处置：seq-1 由 dismissed 改为 resolved（原结论录错）。"""
    return [
        {"seq": 0, "decision": "resolved",
         "handled_at": iso(300), "note": "现场已复冷"},
        {"seq": 1, "decision": "resolved",
         "handled_at": iso(1300), "note": "更正：确认为真实越界，已处置"},
    ]


def correction_body(original, new, digest, decisions):
    return {
        "original_case_id": original,
        "new_case_id": new,
        "analysis_digest": digest,
        "decisions": decisions,
    }


def review_body(case_id, batch_id, digest, decisions):
    return {
        "batch_id": batch_id,
        "case_id": case_id,
        "analysis_digest": digest,
        "decisions": decisions,
    }


class CorrectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 与其他测试模块同进程运行时库路径可能已被固定，显式指向本模块独立库。
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)

        # 批次 C-001：2 条异常；CASE-ORIG 已 closed（seq-1 误录为 dismissed）
        cls.digest = cls._analyze(make_payload("C-001"))
        cls._review("CASE-ORIG", "C-001", cls.digest, decisions_closed())

        # 批次 C-SHIP：2 条异常；CASE-SHIP-1 已 closed，且被 SHP-1/SHP-2 引用
        cls.digest_ship = cls._analyze(make_payload("C-SHIP"))
        cls._review("CASE-SHIP-1", "C-SHIP", cls.digest_ship,
                    decisions_closed())
        cls._shipment("SHP-1", "C-SHIP", cls.digest_ship, "CASE-SHIP-1",
                      iso(10000))
        cls._shipment("SHP-2", "C-SHIP", cls.digest_ship, "CASE-SHIP-1",
                      iso(20000))

    @classmethod
    def _analyze(cls, payload):
        r = cls.client.post("/api/v1/batches/analyze", json=payload)
        assert r.status_code == 200, r.text
        return r.json()["analysis_digest"]

    @classmethod
    def _review(cls, case_id, batch_id, digest, decisions):
        r = cls.client.post(
            "/api/v1/cases/review",
            json=review_body(case_id, batch_id, digest, decisions),
        )
        assert r.status_code == 200, r.text
        return r.json()

    @classmethod
    def _shipment(cls, shipment_id, batch_id, digest, case_id, handoff_at):
        r = cls.client.post("/api/v1/shipments/snapshot", json={
            "shipment_id": shipment_id,
            "batches": [
                {"batch_id": batch_id, "analysis_digest": digest,
                 "case_id": case_id, "handoff_at": handoff_at},
            ],
        })
        assert r.status_code == 200, r.text
        assert r.json()["result"] == "released", r.text
        return r.json()

    # ---------- 工具 ----------

    def _counts(self):
        with sqlite3.connect(_TMP_DB) as conn:
            cases = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]
            corrections = conn.execute(
                "SELECT COUNT(*) FROM case_corrections").fetchone()[0]
        return cases, corrections

    def _correction_row(self, original_case_id):
        with sqlite3.connect(_TMP_DB) as conn:
            return conn.execute(
                """SELECT new_case_id FROM case_corrections
                   WHERE original_case_id = ?""",
                (original_case_id,),
            ).fetchone()

    # ---------- 用例 ----------

    def test_01_correction_creates_replacement_case(self):
        cases_before, corrections_before = self._counts()
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-ORIG", "CASE-FIXED", self.digest,
                                 decisions_corrected()),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "MISS")
        body = r.json()
        self.assertFalse(body["cached"])
        self.assertEqual(body["original_case_id"], "CASE-ORIG")
        self.assertEqual(body["case_id"], "CASE-FIXED")
        self.assertEqual(body["batch_id"], "C-001")
        self.assertEqual(body["analysis_digest"], self.digest)
        self.assertEqual(body["status"], "closed")
        self.assertEqual([d["seq"] for d in body["decisions"]], [0, 1])
        self.assertEqual(body["decisions"][1]["decision"], "resolved")
        self.assertEqual(body["decisions"][1]["note"],
                         "更正：确认为真实越界，已处置")
        # 原案件未被任何联单引用
        self.assertEqual(body["affected_shipment_ids"], [])

        cases_after, corrections_after = self._counts()
        self.assertEqual(cases_after, cases_before + 1)
        self.assertEqual(corrections_after, corrections_before + 1)
        self.assertEqual(self._correction_row("CASE-ORIG")[0], "CASE-FIXED")
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                """SELECT batch_id, analysis_digest, status FROM cases
                   WHERE case_id = 'CASE-FIXED'""",
            ).fetchone()
            decision_rows = conn.execute(
                """SELECT COUNT(*) FROM case_decisions
                   WHERE case_id = 'CASE-FIXED'""",
            ).fetchone()[0]
        self.assertEqual(tuple(row), ("C-001", self.digest, "closed"))
        self.assertEqual(decision_rows, 2)

    def test_02_same_correction_replays_idempotently(self):
        body = correction_body("CASE-ORIG", "CASE-FIXED", self.digest,
                               decisions_corrected())
        cases_before, corrections_before = self._counts()
        r = self.client.post("/api/v1/cases/correction", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "HIT")
        self.assertTrue(r.json()["cached"])
        self.assertEqual(self._counts(), (cases_before, corrections_before))

        # 处置顺序不影响幂等判定（内容哈希按 seq 排序）
        reordered = json.loads(json.dumps(body))
        reordered["decisions"] = list(reversed(body["decisions"]))
        r2 = self.client.post("/api/v1/cases/correction", json=reordered)
        self.assertEqual(r2.status_code, 200, r2.text)
        self.assertTrue(r2.json()["cached"])
        self.assertEqual([d["seq"] for d in r2.json()["decisions"]], [0, 1])
        self.assertEqual(self._counts(), (cases_before, corrections_before))

    def test_03_same_original_different_content_conflicts(self):
        cases_before, corrections_before = self._counts()
        # 同一原案件 + 不同新案件号
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-ORIG", "CASE-OTHER", self.digest,
                                 decisions_corrected()),
        )
        self.assertEqual(r.status_code, 409)
        # 同一原案件 + 同一新案件号 + 不同处置内容
        changed = correction_body("CASE-ORIG", "CASE-FIXED", self.digest,
                                  decisions_corrected())
        changed["decisions"][0]["note"] = "另一个说法"
        r = self.client.post("/api/v1/cases/correction", json=changed)
        self.assertEqual(r.status_code, 409)
        # 同一原案件 + 不同摘要
        changed = correction_body("CASE-ORIG", "CASE-FIXED", "0" * 64,
                                  decisions_corrected())
        r = self.client.post("/api/v1/cases/correction", json=changed)
        self.assertEqual(r.status_code, 409)
        self.assertEqual(self._counts(), (cases_before, corrections_before))
        self.assertIsNotNone(self._correction_row("CASE-ORIG"))
        self.assertEqual(self._correction_row("CASE-ORIG")[0], "CASE-FIXED")

    def test_04_new_case_id_occupied_conflicts(self):
        self._review("CASE-B2", "C-001", self.digest, decisions_closed())
        cases_before, corrections_before = self._counts()
        # 新案件号已被其他案件占用
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-B2", "CASE-ORIG", self.digest,
                                 decisions_corrected()),
        )
        self.assertEqual(r.status_code, 409)
        # 新案件号与原案件号相同（模型层拒绝）
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-B2", "CASE-B2", self.digest,
                                 decisions_corrected()),
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(self._counts(), (cases_before, corrections_before))

    def test_05_unknown_original_case_404(self):
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-NOPE", "CASE-NEW", self.digest,
                                 decisions_corrected()),
        )
        self.assertEqual(r.status_code, 404)

    def test_06_stale_digest_conflicts_and_persists_nothing(self):
        counts_before = self._counts()
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-B2", "CASE-B2-FIX", "0" * 64,
                                 decisions_corrected()),
        )
        self.assertEqual(r.status_code, 409)
        self.assertEqual(self._counts(), counts_before)
        with sqlite3.connect(_TMP_DB) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM cases WHERE case_id = 'CASE-B2-FIX'"
                ).fetchone()
            )

    def test_07_decision_validation_422(self):
        counts_before = self._counts()
        # 缺序号 1
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-B2", "CASE-B2-FIX", self.digest,
                                 [decisions_corrected()[0]]),
        )
        self.assertEqual(r.status_code, 422)
        # dismissed 缺说明
        bad = [
            {"seq": 0, "decision": "resolved", "handled_at": iso(300)},
            {"seq": 1, "decision": "dismissed", "handled_at": iso(1300)},
        ]
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-B2", "CASE-B2-FIX", self.digest, bad),
        )
        self.assertEqual(r.status_code, 422)
        # handled_at 早于异常结束
        bad = [
            {"seq": 0, "decision": "resolved", "handled_at": iso(239)},
            decisions_corrected()[1],
        ]
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-B2", "CASE-B2-FIX", self.digest, bad),
        )
        self.assertEqual(r.status_code, 422)
        # 序号重复（模型层拒绝）
        dup = [decisions_corrected()[0], decisions_corrected()[0]]
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-B2", "CASE-B2-FIX", self.digest, dup),
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(self._counts(), counts_before)

    def test_08_correction_chain_and_single_replacement(self):
        # 更正案件本身可继续更正：CASE-FIXED → CASE-FIXED-2（含 accepted → open）
        chain_decisions = [
            {"seq": 0, "decision": "accepted", "handled_at": iso(300)},
            {"seq": 1, "decision": "resolved",
             "handled_at": iso(1300), "note": "再次更正：已处置"},
        ]
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-FIXED", "CASE-FIXED-2", self.digest,
                                 chain_decisions),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "open")
        self.assertEqual(self._correction_row("CASE-FIXED")[0],
                         "CASE-FIXED-2")
        # 已被替代的案件不能再次更正（无论用哪个新案件号）
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-ORIG", "CASE-ORIG-3", self.digest,
                                 decisions_corrected()),
        )
        self.assertEqual(r.status_code, 409)
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-FIXED", "CASE-FIXED-3", self.digest,
                                 chain_decisions),
        )
        self.assertEqual(r.status_code, 409)

    def test_09_correction_response_lists_shipments_sorted(self):
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-SHIP-1", "CASE-SHIP-2",
                                 self.digest_ship, decisions_corrected()),
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["batch_id"], "C-SHIP")
        self.assertEqual(body["status"], "closed")
        # 直接引用原案件的联单按联单号排序列出，供重新出具
        self.assertEqual(body["affected_shipment_ids"], ["SHP-1", "SHP-2"])

    def test_10_superseded_case_rejected_for_new_shipment(self):
        r = self.client.post("/api/v1/shipments/snapshot", json={
            "shipment_id": "SHP-3-STALE",
            "batches": [
                {"batch_id": "C-SHIP", "analysis_digest": self.digest_ship,
                 "case_id": "CASE-SHIP-1", "handoff_at": iso(30000)},
            ],
        })
        self.assertEqual(r.status_code, 409)
        self.assertIn("CASE-SHIP-2", r.json()["detail"])
        with sqlite3.connect(_TMP_DB) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM shipments WHERE shipment_id = 'SHP-3-STALE'"
                ).fetchone()
            )

    def test_11_existing_shipments_keep_historical_snapshot(self):
        # 已有联单同号回放：仍返回原案件的历史快照，不受更正影响
        body = {
            "shipment_id": "SHP-1",
            "batches": [
                {"batch_id": "C-SHIP", "analysis_digest": self.digest_ship,
                 "case_id": "CASE-SHIP-1", "handoff_at": iso(10000)},
            ],
        }
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "HIT")
        payload = r.json()
        self.assertTrue(payload["cached"])
        self.assertEqual(payload["result"], "released")
        self.assertEqual(payload["batches"][0]["case_id"], "CASE-SHIP-1")
        self.assertEqual(payload["batches"][0]["case_status"], "closed")
        # 同号换内容依旧 409（联单不可变更）
        changed = json.loads(json.dumps(body))
        changed["batches"][0]["case_id"] = "CASE-SHIP-2"
        r = self.client.post("/api/v1/shipments/snapshot", json=changed)
        self.assertEqual(r.status_code, 409)

    def test_12_replacement_case_usable_in_new_shipment(self):
        r = self.client.post("/api/v1/shipments/snapshot", json={
            "shipment_id": "SHP-3",
            "batches": [
                {"batch_id": "C-SHIP", "analysis_digest": self.digest_ship,
                 "case_id": "CASE-SHIP-2", "handoff_at": iso(30000)},
            ],
        })
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["result"], "released")
        self.assertEqual(r.json()["batches"][0]["case_id"], "CASE-SHIP-2")
        self.assertEqual(r.json()["batches"][0]["case_status"], "closed")

    def test_13_review_endpoint_still_replays_old_cases(self):
        # 原复核接口回放被替代案件：同内容依旧幂等
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-ORIG", "C-001", self.digest,
                             decisions_closed()),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "HIT")
        self.assertEqual(r.json()["case_id"], "CASE-ORIG")
        # 复核接口回放更正落库的新案件（同一内容哈希口径）
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-FIXED", "C-001", self.digest,
                             decisions_corrected()),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.json()["cached"])
        # 被替代案件换内容提交依旧 409
        changed = review_body("CASE-ORIG", "C-001", self.digest,
                              decisions_corrected())
        r = self.client.post("/api/v1/cases/review", json=changed)
        self.assertEqual(r.status_code, 409)

    def test_14_zero_anomaly_case_correction(self):
        digest_zero = self._analyze(make_normal_payload())
        self._review("CASE-ZERO", "C-ZERO", digest_zero, [])
        r = self.client.post(
            "/api/v1/cases/correction",
            json=correction_body("CASE-ZERO", "CASE-ZERO-2", digest_zero, []),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "closed")
        self.assertEqual(r.json()["decisions"], [])

    def test_15_concurrent_corrections_only_one_wins(self):
        self._review("CASE-RACE", "C-001", self.digest, decisions_closed())
        bodies = [
            correction_body("CASE-RACE", "CASE-RACE-A", self.digest,
                            decisions_corrected()),
            correction_body("CASE-RACE", "CASE-RACE-B", self.digest,
                            decisions_corrected()),
        ]
        results = []

        def submit(body):
            results.append(
                self.client.post("/api/v1/cases/correction", json=body)
            )

        threads = [threading.Thread(target=submit, args=(b,)) for b in bodies]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        statuses = sorted(r.status_code for r in results)
        self.assertEqual(statuses, [200, 409])
        # 同一原案件只落库一条替代关系；两个新案件号只有一个存在
        self.assertIsNotNone(self._correction_row("CASE-RACE"))
        with sqlite3.connect(_TMP_DB) as conn:
            existing = {
                row[0]
                for row in conn.execute(
                    """SELECT case_id FROM cases
                       WHERE case_id IN ('CASE-RACE-A', 'CASE-RACE-B')"""
                ).fetchall()
            }
        self.assertEqual(len(existing), 1)

    def test_16_concurrent_same_correction_replays(self):
        self._review("CASE-RACE2", "C-001", self.digest, decisions_closed())
        body = correction_body("CASE-RACE2", "CASE-RACE2-FIX", self.digest,
                               decisions_corrected())
        results = []

        def submit():
            results.append(
                self.client.post("/api/v1/cases/correction", json=body)
            )

        threads = [threading.Thread(target=submit) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual([r.status_code for r in results], [200, 200])
        cached_flags = sorted(r.json()["cached"] for r in results)
        self.assertEqual(cached_flags, [False, True])
        self.assertEqual(self._correction_row("CASE-RACE2")[0],
                         "CASE-RACE2-FIX")


if __name__ == "__main__":
    unittest.main()
