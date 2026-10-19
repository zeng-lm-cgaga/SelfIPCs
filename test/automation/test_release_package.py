"""Release tooling tests use private repositories and never contact GitHub."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
import release_package as release


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.STDOUT,
                                   text=True).strip()


class ReleaseFixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="selfipcs-release-test-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / "repo"
        self.root.mkdir()
        (self.root / "CMakeLists.txt").write_text("project(SelfIPCs VERSION 0.4.0 LANGUAGES CXX)\n")
        (self.root / "README.md").write_text("# SelfIPCs\n")
        (self.root / "scripts").mkdir()
        for name in ("release_package.py", "self_ipcs_compat.py", "publish_release.sh"):
            shutil.copyfile(REPO / "scripts" / name, self.root / "scripts" / name)
        git(self.root, "init", "--initial-branch=main")
        git(self.root, "config", "user.name", "SelfIPCs test")
        git(self.root, "config", "user.email", "test@example.invalid")
        git(self.root, "config", "core.autocrlf", "false")
        self.commit()
        git(self.root, "update-ref", "refs/remotes/origin/main", "HEAD")
        self.sha = git(self.root, "rev-parse", "HEAD")
        self.prefix = self.base / "install"
        (self.prefix / "lib").mkdir(parents=True)
        (self.prefix / "lib/libself_ipcs.a").write_bytes(b"test-archive\n")
        self.build = self.base / "build"
        self.build.mkdir()
        (self.build / "CMakeCache.txt").write_text(
            "CMAKE_BUILD_TYPE:STRING=Release\nCMAKE_CXX_FLAGS_RELEASE:STRING=-O3 -DNDEBUG\n"
            "BUILD_TESTING:BOOL=OFF\nSELFIPCS_ENABLE_FAILPOINTS:BOOL=OFF\n"
            "SELFIPCS_ENABLE_SANITIZERS:BOOL=OFF\nSELFIPCS_WARNINGS_AS_ERRORS:BOOL=ON\n"
            f"CMAKE_CXX_COMPILER:FILEPATH={sys.executable}\n")
        self.output = self.base / "assets"

    def commit(self):
        git(self.root, "add", ".")
        git(self.root, "commit", "-m", "private release test fixture")

    def create(self, full=True, directory=None):
        with patch.object(release.platform, "system", return_value="Linux"), \
                patch.object(release.platform, "machine", return_value="x86_64"):
            return release.create_assets(self.root, self.prefix, self.build,
                                         directory or self.output, full=full)

    def verify(self, **kwargs):
        return release.verify_assets(self.root, self.output, commit=self.sha, **kwargs)


class ReleaseTest(ReleaseFixture, unittest.TestCase):
    def test_roundtrip_and_relocatable_extract(self):
        self.create()
        self.verify(require_full=True, tag="v0.4.0")
        prefix = release.extract_asset(self.root, self.output, self.base / "relocated")
        self.assertEqual((prefix / "lib/libself_ipcs.a").read_bytes(), b"test-archive\n")
        source = release.extract_asset(self.root, self.output, self.base / "source", source=True)
        self.assertEqual(release.project_version(source), "0.4.0")

    def test_archive_bytes_are_reproducible(self):
        first = self.create()
        second = self.create(directory=self.base / "second")
        self.assertEqual(first, second)
        for path in self.output.iterdir():
            self.assertEqual(path.read_bytes(), (self.base / "second" / path.name).read_bytes())

    def test_modified_checkout_refused(self):
        (self.root / "README.md").write_text("modified\n")
        with self.assertRaisesRegex(ValueError, "clean"):
            self.create()

    def test_private_tracked_documents_refused(self):
        (self.root / "AGENTS.md").write_text("private\n")
        self.commit()
        with self.assertRaisesRegex(ValueError, "non-public"):
            release.source_entries(self.root)

    def test_nested_private_documents_refused(self):
        for name in ("docs/design.md", "scripts/AGENTS.md", "src/design.md", "evidence/log.txt",
                     "src/../../secret", "/etc/passwd", "src\\escape.cpp"):
            with self.subTest(name=name):
                self.assertFalse(release.public_source_path(name))
        for name in ("README.md", "src/channel.cpp", "scripts/run.py", ".github/workflows/ci.yml"):
            self.assertTrue(release.public_source_path(name))

    def test_output_directory_never_overwritten(self):
        self.create()
        sentinel = self.output / "sentinel"
        sentinel.write_text("keep")
        with self.assertRaises(FileExistsError):
            self.create()
        self.assertEqual(sentinel.read_text(), "keep")

    def test_tampered_archive_refused(self):
        manifest = self.create()
        name = next(iter(manifest["archives"]))
        with (self.output / name).open("ab") as stream:
            stream.write(b"corrupt")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.verify()

    def test_wrong_commit_and_tag_refused(self):
        self.create()
        with self.assertRaisesRegex(ValueError, "identity"):
            release.verify_assets(self.root, self.output, commit="0" * 40)
        with self.assertRaisesRegex(ValueError, "tag/version"):
            self.verify(tag="v0.4.1")

    def test_smoke_only_assets_cannot_be_published(self):
        self.create(full=False)
        self.verify()
        with self.assertRaisesRegex(ValueError, "full validation"):
            self.verify(require_full=True)

    def test_extra_assets_refused(self):
        self.create()
        (self.output / "design.md").write_text("private")
        with self.assertRaisesRegex(ValueError, "unexpected"):
            self.verify()

    def test_missing_checksum_coverage_refused(self):
        self.create()
        sums = self.output / "SHA256SUMS"
        sums.write_text(sums.read_text().splitlines()[0] + "\n")
        with self.assertRaisesRegex(ValueError, "cover exactly"):
            self.verify()

    def test_checksum_path_traversal_refused(self):
        self.create()
        (self.output / "SHA256SUMS").write_text("0" * 64 + "  ../secret\n")
        with self.assertRaisesRegex(ValueError, "invalid"):
            self.verify()

    def test_release_requires_non_sanitized_configuration(self):
        cache = self.build / "CMakeCache.txt"
        cache.write_text(cache.read_text().replace("SELFIPCS_ENABLE_SANITIZERS:BOOL=OFF",
                                                  "SELFIPCS_ENABLE_SANITIZERS:BOOL=ON"))
        with self.assertRaisesRegex(ValueError, "SANITIZERS"):
            self.create()

    def test_tag_matches_version_and_main_history(self):
        git(self.root, "tag", "v0.4.0")
        release.check_tag(self.root, "v0.4.0")
        with self.assertRaisesRegex(ValueError, "project version"):
            release.check_tag(self.root, "v0.4.1")

    def test_tag_off_main_refused(self):
        (self.root / "README.md").write_text("unmerged feature\n")
        self.commit()
        git(self.root, "tag", "v0.4.0")
        with self.assertRaises(subprocess.CalledProcessError):
            release.check_tag(self.root, "v0.4.0")


class ArchiveSafetyTest(unittest.TestCase):
    def validate(self, entries, source=False):
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as archive:
            for name, kind, target in entries:
                info = tarfile.TarInfo(name)
                info.type, info.linkname = kind, target
                archive.addfile(info)
        stream.seek(0)
        with tarfile.open(fileobj=stream) as archive:
            release.validate_archive(archive, "sdk", source=source)

    def test_safe_regular_file(self):
        self.validate([("sdk/include/api.hpp", tarfile.REGTYPE, "")])

    def test_escape_absolute_duplicate_and_devices_refused(self):
        for entries in ([('/etc/passwd', tarfile.REGTYPE, '')],
                        [('sdk/../escape', tarfile.REGTYPE, '')],
                        [('sdk/a', tarfile.REGTYPE, ''), ('sdk/a', tarfile.REGTYPE, '')],
                        [('sdk/device', tarfile.CHRTYPE, '')],
                        [('sdk/link', tarfile.SYMTYPE, '../outside')],
                        [('sdk/link', tarfile.LNKTYPE, 'sdk/a')]):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                self.validate(entries)

    def test_safe_tool_compatibility_link(self):
        self.validate([('sdk/bin/new', tarfile.REGTYPE, ''), ('sdk/bin/old', tarfile.SYMTYPE, 'new')])

    def test_traversing_symlink_refused(self):
        with self.assertRaisesRegex(ValueError, "traverses"):
            self.validate([('sdk/dir', tarfile.DIRTYPE, ''), ('sdk/link', tarfile.SYMTYPE, 'dir'),
                           ('sdk/link/file', tarfile.REGTYPE, '')])

    def test_dangling_symlink_refused(self):
        with self.assertRaisesRegex(ValueError, "dangling"):
            self.validate([('sdk/link', tarfile.SYMTYPE, 'missing')])

    def test_source_documents_refused(self):
        with self.assertRaisesRegex(ValueError, "private document"):
            self.validate([('sdk/docs/design.md', tarfile.REGTYPE, '')], source=True)


@unittest.skipUnless(sys.platform.startswith("linux"), "publication shell tests require Linux")
class PublicationTest(ReleaseFixture, unittest.TestCase):
    # This class reuses the private fixture, not the package test cases.
    def setUp(self):
        super().setUp()
        git(self.root, "tag", "v0.4.0")
        self.create()
        mock_bin = self.base / "mock-bin"
        mock_bin.mkdir()
        self.events = self.base / "gh-events"
        fake = mock_bin / "gh"
        fake.write_text('''#!/usr/bin/env python3
import json, os, pathlib, shutil, sys
a = sys.argv[1:]
with open(os.environ['MOCK_EVENTS'], 'a') as stream:
    stream.write(json.dumps(a) + '\\n')
if a[0] == 'api':
    print('0' * 40 if os.environ.get('MOCK_TAG_MOVED') else os.environ['RELEASE_COMMIT'])
elif a[:2] == ['release', 'view']:
    sys.exit(0 if os.environ.get('MOCK_EXISTS') else 1)
elif a[:2] == ['release', 'download']:
    dest = pathlib.Path(a[a.index('--dir') + 1])
    for path in pathlib.Path(os.environ['RELEASE_ASSETS']).iterdir():
        shutil.copyfile(path, dest / path.name)
    if os.environ.get('MOCK_CORRUPT'):
        (dest / 'release-manifest.json').write_text('{}')
elif a[:2] not in (['release', 'create'], ['release', 'edit']):
    sys.exit(2)
''')
        fake.chmod(0o755)
        self.env = os.environ | {"PATH": str(mock_bin) + os.pathsep + os.environ["PATH"],
                                 "GH_TOKEN": "test-token", "GITHUB_REPOSITORY": "test/SelfIPCs",
                                 "RELEASE_TAG": "v0.4.0", "RELEASE_COMMIT": self.sha,
                                 "RELEASE_ASSETS": str(self.output), "RUNNER_TEMP": str(self.base),
                                 "MOCK_EVENTS": str(self.events), "PYTHONDONTWRITEBYTECODE": "1"}

    def invoke(self, **env):
        result = subprocess.run(["bash", str(self.root / "scripts/publish_release.sh")],
                                env=self.env | env, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=30)
        events = [json.loads(line) for line in self.events.read_text().splitlines()]
        return result, [a[:2] for a in events]

    def test_publication_is_last_after_download_verification(self):
        result, events = self.invoke()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(events, [['api', 'repos/test/SelfIPCs/commits/v0.4.0'], ['release', 'view'],
                                  ['release', 'create'], ['release', 'download'],
                                  ['api', 'repos/test/SelfIPCs/commits/v0.4.0'], ['release', 'edit']])

    def test_existing_release_is_not_overwritten(self):
        result, events = self.invoke(MOCK_EXISTS="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(['release', 'create'], events)
        self.assertNotIn(['release', 'edit'], events)

    def test_failed_download_verification_keeps_draft(self):
        result, events = self.invoke(MOCK_CORRUPT="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(['release', 'create'], events)
        self.assertNotIn(['release', 'edit'], events)

    def test_moved_tag_is_not_published(self):
        result, events = self.invoke(MOCK_TAG_MOVED="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(['release', 'create'], events)
        self.assertNotIn(['release', 'edit'], events)


if __name__ == "__main__":
    unittest.main()
