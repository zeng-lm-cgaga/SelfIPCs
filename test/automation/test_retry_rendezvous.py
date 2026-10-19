"""The retry hold must begin after both native rendezvous conditions."""
from pathlib import Path
import signal
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import test_mpmc_payload_retry as retry


class RetryRendezvousTest(unittest.TestCase):
    def test_startup_delay_does_not_consume_measured_hold(self):
        marker = Mock()
        marker.is_file.side_effect = [False, False, True]
        marker.read_text.return_value = "RETRY_READY pid=20 sequence=1\n"
        with patch.object(retry, "children_of", return_value=[10]), \
                patch.object(retry, "has_role", return_value=True), \
                patch.object(Path, "read_text", return_value="State:\tT (stopped)\n"), \
                patch.object(retry.time, "sleep") as sleep, \
                patch.object(retry.os, "kill") as kill:
            self.assertEqual(retry.stop_consumer(1, marker, 1)[0], 10)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.001, 0.001, 0.15])
            kill.assert_called_once_with(10, signal.SIGCONT)

    def test_running_consumer_is_not_mistaken_for_a_stopped_one(self):
        marker = Mock()
        with patch.object(retry, "children_of", return_value=[10]), \
                patch.object(retry, "has_role", return_value=True), \
                patch.object(Path, "read_text", return_value="State:\tR (running)\n"), \
                patch.object(retry.time, "monotonic", side_effect=[0, 0, 2]), \
                patch.object(retry.time, "sleep"), patch.object(retry.os, "kill") as kill:
            with self.assertRaisesRegex(RuntimeError, "rendezvous"):
                retry.stop_consumer(1, marker, 1)
            marker.is_file.assert_not_called()
            kill.assert_not_called()

    def test_timeout_resumes_only_the_observed_stopped_consumer(self):
        marker = Mock()
        marker.is_file.return_value = False
        with patch.object(retry, "children_of", return_value=[10]), \
                patch.object(retry, "has_role", return_value=True), \
                patch.object(Path, "read_text", return_value="State:\tT (stopped)\n"), \
                patch.object(retry.time, "monotonic", side_effect=[0, 0, 2]), \
                patch.object(retry.time, "sleep"), patch.object(retry.os, "kill") as kill:
            with self.assertRaisesRegex(RuntimeError, "rendezvous"):
                retry.stop_consumer(1, marker, 1)
            kill.assert_called_once_with(10, signal.SIGCONT)


if __name__ == "__main__":
    unittest.main()
