#!/usr/bin/env python3
"""Check that the image build never reuses a stale workspace crate from the BuildKit target cache.

A build context whose tar entries all have mtime=0 (a reproducible source snapshot) looks older to
Cargo than the cached artifacts, so Cargo may skip a changed crate. Each probe below builds such a
context on one private buildx builder, after the first build has filled the target cache.

  P0  base source builds (fills the cache)
  P1  compile_error! in server/git MUST fail, with its token in the log
  P2  a token in a library crate the scion binary keeps MUST build, and the binary MUST hold it
  P3  panic! in common/protocol/build.rs MUST fail
  P4  the base source built twice more gives the same sha256 for scion and scn
  P5  two concurrent builds (tokens A and B) both build, and each binary holds only its own token
  P6  no probe after P0 compiles a crates.io dependency, and the target cache mount is locked

Usage: python3 scripts/check-image-freshness.py   (exit 0: all probes pass; exit 1: a probe failed)
Needs docker with buildx. The first run compiles all dependencies (about 10 to 15 minutes).
"""
import concurrent.futures
import hashlib
import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Exports only the binaries that the build stage leaves in /out
OUT_STAGE = "\nFROM scratch AS freshness-out\nCOPY --from=build /out/ /\n"
GIT_LIB = "server/git/src/lib.rs"
ROUTE = "pub fn route(method: &Method, path: &str, headers: &HeaderMap) -> Option<(String, Endpoint)> {\n"
BUILD_RS = "common/protocol/build.rs"
MAIN = "fn main() -> Result<(), Box<dyn std::error::Error>> {\n"

failures = []


def check(name, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'} {name} {detail}", flush=True)
    if not ok:
        failures.append(name)


def source_files():
    names = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, check=True, capture_output=True).stdout
    return [n for n in names.decode().split("\0") if n and os.path.isfile(os.path.join(ROOT, n))]


def snapshot(edits):
    """The tracked tree as a tar with every mtime 0. `edits` maps a path to a function on its text."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name in sorted(source_files()):
            with open(os.path.join(ROOT, name), "rb") as f:
                data = f.read()
            if name == "Dockerfile":
                data += OUT_STAGE.encode()
            if name in edits:
                text = edits[name](data.decode())
                data = text.encode()
            info = tarfile.TarInfo(name)
            info.size, info.mtime, info.mode = len(data), 0, 0o644
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def replace(old, new):
    def edit(text):
        assert old in text, f"anchor not found: {old!r}"
        return text.replace(old, new, 1)
    return edit


def after(anchor, add):
    return replace(anchor, anchor + add)


def build(builder, edits, out):
    """Build the snapshot; return (exit code, log). The binaries land in `out`."""
    p = subprocess.run(
        ["docker", "buildx", "build", "--builder", builder, "--progress", "plain", "--target", "freshness-out",
         "--build-arg", "CARGO_PROFILE=dev", "--output", f"type=local,dest={out}", "-"],
        input=snapshot(edits), capture_output=True)
    return p.returncode, p.stdout.decode(errors="replace") + p.stderr.decode(errors="replace")


def sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def holds(path, token):
    with open(path, "rb") as f:
        return token.encode() in f.read()


def token_edit(token):
    return {GIT_LIB: after(ROUTE, f'    std::hint::black_box("{token}");\n')}


def dependency_compiles(log):
    """Names of crates.io dependencies compiled in `log`; workspace crates print a path."""
    return re.findall(r"Compiling (\S+ v\S+)\s*$", log, re.M)


def main():
    dockerfile = open(os.path.join(ROOT, "Dockerfile")).read()
    target_mounts = [l for l in dockerfile.splitlines() if "target=/src/target" in l]
    check("P6 target cache mount is locked", len(target_mounts) == 1 and "sharing=locked" in target_mounts[0])

    builder = "freshness-" + uuid.uuid4().hex[:8]
    subprocess.run(["docker", "buildx", "create", "--name", builder, "--driver", "docker-container"],
                   check=True, capture_output=True)
    work = tempfile.mkdtemp(prefix="freshness-")
    logs = []
    try:
        def out(name):
            return os.path.join(work, name)

        code, log = build(builder, {}, out("p0"))
        check("P0 base builds", code == 0, "" if code == 0 else log[-3000:])
        if code != 0:
            return

        tok = "FRESHNESS_P1_" + uuid.uuid4().hex
        code, log = build(builder, {GIT_LIB: lambda t: t + f'\ncompile_error!("{tok}");\n'}, out("p1"))
        logs.append(log)
        check("P1 compile_error in server/git fails", code != 0 and tok in log, f"exit {code}")

        tok = "FRESHNESS_P2_" + uuid.uuid4().hex
        code, log = build(builder, token_edit(tok), out("p2"))
        logs.append(log)
        check("P2 library change reaches scion", code == 0 and holds(out("p2") + "/scion", tok), f"exit {code}")

        tok = "FRESHNESS_P3_" + uuid.uuid4().hex
        code, log = build(builder, {BUILD_RS: after(MAIN, f'    if true {{ panic!("{tok}"); }}\n')}, out("p3"))
        logs.append(log)
        check("P3 build.rs panic fails", code != 0 and tok in log, f"exit {code}")

        shas = []
        for i in (1, 2):
            code, log = build(builder, {}, out(f"p4-{i}"))
            logs.append(log)
            shas.append((code, sha(out(f"p4-{i}") + "/scion"), sha(out(f"p4-{i}") + "/scn")) if code == 0 else (code,))
        check("P4 same source twice gives equal binaries",
              all(s[0] == 0 for s in shas) and shas[0] == shas[1], str(shas))

        ta, tb = ("FRESHNESS_P5A_" + uuid.uuid4().hex, "FRESHNESS_P5B_" + uuid.uuid4().hex)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            fa = pool.submit(build, builder, token_edit(ta), out("p5a"))
            fb = pool.submit(build, builder, token_edit(tb), out("p5b"))
            (ca, la), (cb, lb) = fa.result(), fb.result()
        logs += [la, lb]
        ok = ca == 0 and cb == 0 and holds(out("p5a") + "/scion", ta) and holds(out("p5b") + "/scion", tb) \
            and not holds(out("p5a") + "/scion", tb) and not holds(out("p5b") + "/scion", ta)
        check("P5 concurrent builds keep their own source", ok, f"exits {ca} {cb}")

        deps = sorted({d for l in logs for d in dependency_compiles(l)})
        check("P6 dependency cache kept after P0", not deps, ", ".join(deps[:10]))
    finally:
        subprocess.run(["docker", "buildx", "rm", "--force", builder], capture_output=True)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
    sys.exit(1 if failures else 0)
