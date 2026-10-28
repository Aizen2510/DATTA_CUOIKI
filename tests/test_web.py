"""Kiểm tra hợp đồng web và lỗi pipeline, không ghi Warehouse."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import web_app as web


class WebTests(unittest.TestCase):
    def setUp(self):
        self.client = web.app.test_client()
        self.headers = {"X-Demo-Request": "1"}
        web.job.clear()
        web.job.update(status="idle", steps=[])

    def test_page_and_local_access(self):
        self.assertEqual(self.client.get("/").status_code, 200)
        self.assertEqual(self.client.get("/", headers={"Host": "example.com"}).status_code, 403)

    def test_reject_unknown_command_and_missing_header(self):
        self.assertEqual(self.client.post("/api/jobs", json={"action": "train"}).status_code, 403)
        for body in [{"action": "rm -rf /"}, {"action": "train", "epochs": 0},
                     {"action": "train", "seed": True}, []]:
            self.assertEqual(self.client.post("/api/jobs", json=body, headers=self.headers).status_code, 400)

    def test_parallel_jobs_and_demo_blocked(self):
        web.job["status"] = "running"
        self.assertEqual(self.client.post("/api/jobs", json={"action": "train"}, headers=self.headers).status_code, 409)
        self.assertEqual(self.client.get("/api/recommendations?user_id=15").status_code, 409)

    def test_missing_model_clear_error(self):
        with tempfile.TemporaryDirectory() as d, patch.object(web, "ARTIFACTS", Path(d)):
            response = self.client.get("/api/recommendations?user_id=15")
            self.assertEqual(response.status_code, 400)
            self.assertIn("Chưa có model", response.json["error"])

    def test_bad_user_input(self):
        for query in ["user_id=abc", "user_id=-1", "user_id=15&top_k=100"]:
            self.assertEqual(self.client.get("/api/recommendations?" + query).status_code, 400)

    def test_failed_step_stops_pipeline(self):
        with tempfile.TemporaryDirectory() as d, patch.object(web, "ARTIFACTS", Path(d)):
            path = Path(d) / "test.log"
            path.touch()
            web.job.update(status="running", steps=[{"status": "pending"}, {"status": "pending"}])
            with patch.object(web, "check_raw", side_effect=ValueError("missing raw")), patch.object(web.subprocess, "Popen") as process:
                web.run_job([("Raw", None), ("Train", ["python"])], path)
                process.assert_not_called()
            self.assertEqual(web.job["status"], "error")
            self.assertEqual(web.job["steps"][1]["status"], "pending")

    def test_log_redaction(self):
        with patch.dict(web.os.environ, {"DW_PASSWORD": "never-show-this"}):
            self.assertNotIn("never-show-this", web.safe_text("password: never-show-this"))
            self.assertNotIn("secret", web.safe_text("postgresql://user:secret@localhost/db"))


if __name__ == "__main__":
    unittest.main()
