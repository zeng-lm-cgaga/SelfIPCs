#!/usr/bin/env python3
"""Create and verify versioned, code-only SelfIPCs release assets."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import pathlib
import platform
import re
import subprocess
import sys
import tarfile

from self_ipcs_compat import validate_release_cache


VERSION = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")
COMMIT = re.compile(r"[0-9a-f]{40}")
SOURCE_DIRS = {".github", "async", "cmake", "examples", "include", "kernel", "scripts", "src", "test", "tools"}
SOURCE_FILES = {".clang-format", ".editorconfig", ".gitignore", "CMakeLists.txt",
                "CMakePresets.json", "README.md", "LICENSE", "LICENSE.txt"}


def git(root: pathlib.Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def project_version(root: pathlib.Path) -> str:
    text = (root / "CMakeLists.txt").read_text(encoding="utf-8")
    match = re.search(r"project\(\s*SelfIPCs\s+VERSION\s+(\S+)", text)
    if not match or not VERSION.fullmatch(match[1]):
        raise ValueError("SelfIPCs requires a numeric major.minor.patch CMake version")
    return match[1]


def public_source_path(name: str) -> bool:
    path = pathlib.PurePosixPath(name)
    if not path.parts or path.is_absolute() or ".." in path.parts or "\\" in name:
        return False
    if path.name in {"AGENTS.md", "CLAUDE.md"} or "__pycache__" in path.parts:
        return False
    if path.suffix.lower() in {".md", ".rst"} and name != "README.md":
        return False
    return name in SOURCE_FILES or (len(path.parts) > 1 and path.parts[0] in SOURCE_DIRS)


def source_entries(root: pathlib.Path) -> list[tuple[str, str, str]]:
    raw = subprocess.check_output(["git", "-C", str(root), "ls-tree", "-rz", "HEAD"])
    entries = []
    for record in raw.split(b"\0"):
        if not record:
            continue
        metadata, name_bytes = record.split(b"\t", 1)
        mode, kind, oid = metadata.decode("ascii").split()
        name = name_bytes.decode("utf-8")
        # Empty evidence placeholders from the historical baseline carry no release content.
        if name in {"evidence/.gitkeep", "evidence/.gitignore"}:
            continue
        if not public_source_path(name) or kind != "blob" or mode not in {"100644", "100755"}:
            raise ValueError(f"non-public or unsupported tracked release path: {name}")
        entries.append((name, oid, mode))
    if not entries:
        raise ValueError("empty release source tree")
    return entries


def check_tag(root: pathlib.Path, tag: str) -> None:
    expected = "v" + project_version(root)
    if tag != expected:
        raise ValueError(f"tag {tag!r} must equal the CMake project version {expected!r}")
    head = git(root, "rev-parse", "HEAD")
    if git(root, "rev-parse", f"refs/tags/{tag}^{{commit}}") != head:
        raise ValueError("release tag does not identify the checked-out commit")
    subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", head,
                    "refs/remotes/origin/main"], check=True)
    source_entries(root)


def digest(path: pathlib.Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def add_bytes(archive: tarfile.TarFile, name: str, data: bytes, epoch: int,
              mode: int = 0o644) -> None:
    info = tarfile.TarInfo(name)
    info.size, info.mtime, info.mode = len(data), epoch, mode
    archive.addfile(info, io.BytesIO(data))


def write_archive(path: pathlib.Path, epoch: int, populate) -> None:
    with path.open("xb") as stream:
        with gzip.GzipFile(filename="", fileobj=stream, mode="wb", mtime=epoch) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                populate(archive)


def validate_archive(archive: tarfile.TarFile, top: str, *, source: bool = False) -> None:
    seen, links = set(), set()
    members = archive.getmembers()
    if not members:
        raise ValueError("empty release archive")
    for member in members:
        path = pathlib.PurePosixPath(member.name)
        if (path.is_absolute() or ".." in path.parts or "\\" in member.name
                or not path.parts or path.parts[0] != top or member.name in seen):
            raise ValueError(f"unsafe or duplicate archive path: {member.name}")
        seen.add(member.name)
        if not (member.isfile() or member.isdir() or member.issym()):
            raise ValueError(f"unsupported archive entry: {member.name}")
        if member.issym():
            target = pathlib.PurePosixPath(member.linkname)
            if not member.linkname or target.is_absolute() or ".." in target.parts or "\\" in member.linkname:
                raise ValueError(f"unsafe archive link: {member.name}")
            links.add(path)
        if source and not member.isdir() and not public_source_path(str(path.relative_to(top))):
            raise ValueError(f"private document in source archive: {member.name}")
    for member in members:
        if any(parent in links for parent in pathlib.PurePosixPath(member.name).parents):
            raise ValueError(f"archive entry traverses a symlink: {member.name}")
    for member in members:
        if member.issym():
            target = pathlib.PurePosixPath(member.name).parent / member.linkname
            if str(target) not in seen:
                raise ValueError(f"dangling archive link: {member.name}")


def create_assets(root: pathlib.Path, prefix: pathlib.Path, build: pathlib.Path,
                  output: pathlib.Path, *, full: bool) -> dict:
    if git(root, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("release packaging requires a clean tracked checkout")
    entries = source_entries(root)
    cache = validate_release_cache(build / "CMakeCache.txt", require_testing_off=True)
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("prebuilt release assets currently target Linux x86_64")
    if not (prefix / "lib" / "libself_ipcs.a").is_file():
        raise ValueError("missing installed libself_ipcs.a")
    output.mkdir(parents=True, exist_ok=False)
    version, commit = project_version(root), git(root, "rev-parse", "HEAD")
    epoch = int(git(root, "show", "-s", "--format=%ct", "HEAD"))
    compiler = subprocess.check_output([cache["CMAKE_CXX_COMPILER"], "--version"], text=True).splitlines()[0]
    manifest = {"schema": 1, "project": "SelfIPCs", "version": version, "commit": commit,
                "full_validation": full, "platform": "linux-x86_64", "compiler": compiler,
                "libc": list(platform.libc_ver()), "source_date_epoch": epoch, "archives": {}}
    binary_top, source_top = f"SelfIPCs-{version}-linux-x86_64", f"SelfIPCs-{version}-source"

    def binary(archive):
        for path in sorted(prefix.rglob("*")):
            name = f"{binary_top}/{path.relative_to(prefix).as_posix()}"
            info = archive.gettarinfo(str(path), arcname=name)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = epoch
            if info.isfile():
                with path.open("rb") as stream:
                    archive.addfile(info, stream)
            else:
                archive.addfile(info)
        add_bytes(archive, f"{binary_top}/README.md", (root / "README.md").read_bytes(), epoch)

    def source(archive):
        for name, oid, mode in entries:
            data = subprocess.check_output(["git", "-C", str(root), "cat-file", "blob", oid])
            add_bytes(archive, f"{source_top}/{name}", data, epoch, int(mode[-3:], 8))

    for top, populate in ((binary_top, binary), (source_top, source)):
        path = output / f"{top}.tar.gz"
        write_archive(path, epoch, populate)
        manifest["archives"][path.name] = digest(path)
    (output / "release-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    names = sorted([*manifest["archives"], "release-manifest.json"])
    (output / "SHA256SUMS").write_text("".join(f"{digest(output / name)}  {name}\n" for name in names),
                                       encoding="utf-8")
    verify_assets(root, output, commit=commit, require_full=full)
    return manifest


def verify_assets(root: pathlib.Path, directory: pathlib.Path, *, commit: str,
                  tag: str | None = None, require_full: bool = False) -> dict:
    if not COMMIT.fullmatch(commit):
        raise ValueError("expected commit must be a full SHA-1")
    manifest = json.loads((directory / "release-manifest.json").read_text(encoding="utf-8"))
    version = project_version(root)
    if (manifest.get("schema") != 1 or manifest.get("project") != "SelfIPCs"
            or manifest.get("version") != version or manifest.get("commit") != commit
            or manifest.get("platform") != "linux-x86_64"):
        raise ValueError("release manifest identity does not match the checked-out source")
    if tag is not None and tag != f"v{version}":
        raise ValueError("release tag/version mismatch")
    if type(manifest.get("full_validation")) is not bool or (require_full and not manifest["full_validation"]):
        raise ValueError("full validation is required for publication")
    archive_names = {f"SelfIPCs-{version}-linux-x86_64.tar.gz", f"SelfIPCs-{version}-source.tar.gz"}
    expected = archive_names | {"release-manifest.json", "SHA256SUMS"}
    if {p.name for p in directory.iterdir()} != expected or any(p.is_symlink() or not p.is_file()
                                                               for p in directory.iterdir()):
        raise ValueError("unexpected, missing or symlinked release assets")
    if not isinstance(manifest.get("archives"), dict) or set(manifest["archives"]) != archive_names:
        raise ValueError("unexpected release archive list")
    checksums = {}
    for line in (directory / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9_.-]+)", line)
        if not match or match[2] in checksums:
            raise ValueError("invalid or duplicate checksum entry")
        checksums[match[2]] = match[1]
    if set(checksums) != expected - {"SHA256SUMS"}:
        raise ValueError("checksum file does not cover exactly the release assets")
    for name, checksum in checksums.items():
        if digest(directory / name) != checksum:
            raise ValueError(f"release checksum mismatch: {name}")
    for name, checksum in manifest["archives"].items():
        if checksums[name] != checksum:
            raise ValueError(f"manifest checksum mismatch: {name}")
        with tarfile.open(directory / name, "r:gz") as archive:
            validate_archive(archive, name.removesuffix(".tar.gz"), source=name.endswith("-source.tar.gz"))
    return manifest


def extract_asset(root: pathlib.Path, directory: pathlib.Path, destination: pathlib.Path,
                  *, source: bool = False) -> pathlib.Path:
    manifest = verify_assets(root, directory, commit=git(root, "rev-parse", "HEAD"))
    suffix = "source" if source else "linux-x86_64"
    top = f"SelfIPCs-{manifest['version']}-{suffix}"
    destination.mkdir(parents=True, exist_ok=False)
    with tarfile.open(directory / f"{top}.tar.gz", "r:gz") as archive:
        # Every path and link was checked before extraction into this fresh private directory.
        options = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
        archive.extractall(destination, **options)
    return destination / top


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True)
    tag_parser = commands.add_parser("check-tag", allow_abbrev=False)
    tag_parser.add_argument("--tag", required=True)
    verify_parser = commands.add_parser("verify", allow_abbrev=False)
    verify_parser.add_argument("--directory", type=pathlib.Path, required=True)
    verify_parser.add_argument("--commit", required=True)
    verify_parser.add_argument("--tag")
    verify_parser.add_argument("--require-full", action="store_true")
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parent.parent
    try:
        if args.command == "check-tag":
            check_tag(root, args.tag)
        else:
            verify_assets(root, args.directory, commit=args.commit, tag=args.tag,
                          require_full=args.require_full)
    except (ValueError, OSError, KeyError, tarfile.TarError, subprocess.CalledProcessError) as error:
        print(f"[release] ERROR: {error}", file=sys.stderr)
        return 1
    print(f"[release] {args.command}: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
