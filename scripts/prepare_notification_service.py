#!/usr/bin/env python3

import argparse
import grp
from pathlib import Path
import pwd
import re


def main():
    parser = argparse.ArgumentParser(description="Generate reviewable service configuration only")
    parser.add_argument("--user", required=True)
    parser.add_argument("--device-group", required=True)
    parser.add_argument("--executable", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        account = pwd.getpwnam(args.user)
        group = grp.getgrnam(args.device_group)
        primary = grp.getgrgid(account.pw_gid)
    except KeyError as error:
        parser.error(f"account/group must already exist: {error}")
    if account.pw_uid == 0 or group.gr_gid == 0 or primary.gr_gid == 0:
        parser.error("select a non-root service account and device group")
    names = (account.pw_name, group.gr_name, primary.gr_name)
    if any(not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_-]*", name) for name in names):
        parser.error("unsupported account or group name")
    executable = args.executable
    if not executable.is_absolute() or not re.fullmatch(r"/[a-zA-Z0-9_./-]+", str(executable)):
        parser.error("executable must be an absolute path without systemd specifiers or whitespace")
    replacements = {
        "SERVICE_USER": account.pw_name, "SERVICE_UID": str(account.pw_uid),
        "SERVICE_GROUP": primary.gr_name, "DEVICE_GROUP": group.gr_name,
        "EXECUTABLE": str(executable),
    }
    source = Path(__file__).resolve().parents[1] / "examples" / "driver_service"
    args.output.mkdir(parents=True, exist_ok=False)
    for template in sorted(source.glob("*.in")):
        text = template.read_text()
        for key, value in replacements.items():
            text = text.replace("@" + key + "@", value)
        target = args.output / template.name.removesuffix(".in")
        target.write_text(text)
        print(target)
    print("Generated only: administrator reviews and explicitly installs rules, tmpfiles and unit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
