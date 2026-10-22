from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))
import check_optional_sdk


class InstalledSdkChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.prefix = self.root / "prefix"
        (self.prefix / "lib/cmake/SelfIPCs").mkdir(parents=True)
        self.output = self.root / "output"
        self.output.mkdir()
        self.argv = ["check_optional_sdk.py", "--prefix", str(self.prefix),
                     "--output-root", str(self.output)]

    def test_private_header_is_rejected_before_compilation(self):
        header = self.prefix / "include/edge_runtime/detail/private.hpp"
        header.parent.mkdir(parents=True)
        header.write_text("private")
        with patch.object(sys, "argv", self.argv), self.assertRaisesRegex(
                RuntimeError, "private detail headers"):
            check_optional_sdk.main()
        self.assertEqual(len(list(self.output.iterdir())), 1)
        self.assertFalse(list(self.output.rglob("*.command.json")))

    def test_source_path_in_export_is_rejected(self):
        config = self.prefix / "lib/cmake/SelfIPCs/SelfIPCsConfig.cmake"
        config.write_text(str(SCRIPTS.parent))
        with patch.object(sys, "argv", self.argv), self.assertRaisesRegex(
                RuntimeError, "source/build path"):
            check_optional_sdk.main()
        self.assertFalse(list(self.output.rglob("*.command.json")))

    def test_digest_matches_independent_known_value(self):
        sample = self.root / "sample"
        sample.write_bytes(b"abc")
        self.assertEqual(check_optional_sdk.digest(sample),
                         "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")


if __name__ == "__main__":
    unittest.main()
