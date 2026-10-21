#!/usr/bin/env python3

import errno
from pathlib import Path
import unittest
from unittest.mock import patch

import manage_software_notification as manager


class ModuleManagementTest(unittest.TestCase):
    def test_status_never_executes_a_tool(self):
        with patch.object(manager, "status", return_value={"loaded": False}), \
             patch.object(manager, "execute") as execute:
            self.assertFalse(manager.manage("status", None, 1)["executed"])
            execute.assert_not_called()

    def test_load_rejects_existing_module(self):
        with patch.object(manager, "status", return_value={"loaded": True}), \
             patch.object(manager, "execute") as execute:
            with self.assertRaises(FileExistsError):
                manager.manage("load", Path("/unused.ko"), 1)
            execute.assert_not_called()

    def test_load_checks_metadata_before_explicit_insmod(self):
        with patch.object(manager, "status", return_value={"loaded": False}), \
             patch.object(manager, "inspect_module", side_effect=ValueError("vermagic mismatch")), \
             patch.object(manager, "execute") as execute:
            with self.assertRaisesRegex(ValueError, "vermagic"):
                manager.manage("load", Path("/unused.ko"), 1)
            execute.assert_not_called()

    def test_relative_module_is_rejected(self):
        with self.assertRaises(ValueError):
            manager.inspect_module(Path("relative.ko"), 1)

    def test_unload_refuses_active_or_unknown_references(self):
        for references in (1, 10, None):
            with self.subTest(references=references), \
                 patch.object(manager, "status", return_value={"loaded": True, "refcnt": references}), \
                 patch.object(manager, "execute") as execute:
                with self.assertRaises(OSError) as raised:
                    manager.manage("unload", None, 1)
                self.assertEqual(raised.exception.errno, errno.EBUSY)
                execute.assert_not_called()

    def test_normal_unload_has_no_force_flag(self):
        with patch.object(manager, "status", return_value={"loaded": True, "refcnt": 0}), \
             patch.object(manager, "require_privilege"), \
             patch.object(manager, "tool", return_value="/sbin/rmmod"), \
             patch.object(manager, "execute", return_value={"returncode": 0}) as execute:
            self.assertTrue(manager.manage("unload", None, 1)["executed"])
            execute.assert_called_once_with(["/sbin/rmmod", manager.NAME], 1)

    def test_non_root_requires_operator(self):
        with patch.object(manager.os, "geteuid", return_value=1000):
            with self.assertRaises(PermissionError):
                manager.require_privilege()


if __name__ == "__main__":
    unittest.main()
