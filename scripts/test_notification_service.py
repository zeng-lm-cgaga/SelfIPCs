#!/usr/bin/env python3

from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import prepare_notification_service as generator


class NotificationServiceTest(unittest.TestCase):
    def invoke(self, output, user_id=2001, group_id=2002, executable="/opt/demo/bin/notification_demo"):
        args = ["prepare_notification_service.py", "--user", "ipc-service", "--device-group",
                "ipc-notify", "--executable", executable, "--output", str(output)]
        with patch("sys.argv", args), \
             patch.object(generator.pwd, "getpwnam", return_value=SimpleNamespace(
                 pw_uid=user_id, pw_gid=2001, pw_name="ipc-service")), \
             patch.object(generator.grp, "getgrnam", return_value=SimpleNamespace(
                 gr_gid=group_id, gr_name="ipc-notify")), \
             patch.object(generator.grp, "getgrgid", return_value=SimpleNamespace(
                 gr_gid=2001, gr_name="ipc-service")):
            return generator.main()

    def test_generates_group_restricted_configuration_only(self):
        with tempfile.TemporaryDirectory(prefix="selfipcs-service-test-") as directory:
            output = Path(directory) / "configuration"
            self.assertEqual(self.invoke(output), 0)
            self.assertEqual(len(list(output.iterdir())), 3)
            rules = (output / "99-self-ipcs-notification.rules").read_text()
            self.assertIn('GROUP="ipc-notify", MODE="0660"', rules)
            service = (output / "self-ipcs-notification.service").read_text()
            self.assertIn("User=ipc-service", service)
            self.assertIn("SupplementaryGroups=ipc-notify", service)
            self.assertIn("DevicePolicy=closed", service)
            self.assertIn("KillMode=control-group", service)
            self.assertIn("ProtectHome=tmpfs\n", service)
            self.assertIn("BindPaths=/run/user/2001\n", service)
            self.assertIn("ProtectSystem=strict\n", service)
            self.assertIn("ReadWritePaths=/dev/shm /run/user/2001\n", service)
            self.assertNotIn("BindPaths=/run/user\n", service)
            self.assertNotIn("@", service)
            self.assertEqual((output / "self-ipcs-notification.conf").read_text(),
                             "d /run/user/2001 0700 ipc-service ipc-service -\n")

    def test_existing_output_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="selfipcs-service-test-") as directory:
            with self.assertRaises(FileExistsError):
                self.invoke(Path(directory))
            self.assertFalse(list(Path(directory).iterdir()))

    def test_rejects_root_account_and_device_group(self):
        for user, group in ((0, 2002), (2001, 0)):
            with self.subTest(user=user, group=group), self.assertRaises(SystemExit):
                self.invoke(Path("/not-created"), user_id=user, group_id=group)

    def test_rejects_systemd_expansions_and_whitespace(self):
        for executable in ("relative/path", "/opt/%n", "/opt/demo --evil", "/opt/$HOME/demo"):
            with self.subTest(executable=executable), self.assertRaises(SystemExit):
                self.invoke(Path("/not-created"), executable=executable)


if __name__ == "__main__":
    unittest.main()
