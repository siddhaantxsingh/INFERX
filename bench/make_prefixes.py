"""16 shared prompt prefixes (~600 bytes of code each) from the held-out
Python standard-library files, separated by lines of -----."""
import glob
import hashlib
import os
import random
import sys

ROOT = "/usr/lib/python3.12"
files = []
for p in sorted(glob.glob(f"{ROOT}/**/*.py", recursive=True)):
    rel = os.path.relpath(p, ROOT)
    if any(s in rel.split("/") for s in ("test", "tests", "idlelib", "site-packages", "dist-packages")):
        continue
    if int(hashlib.sha256(rel.encode()).hexdigest(), 16) % 50 == 0:
        files.append(p)
rng = random.Random(0)
out = []
while len(out) < 16:
    text = open(rng.choice(files), encoding="utf-8").read()
    if len(text) < 2000:
        continue
    start = text.find("\n", rng.randrange(0, len(text) - 1200)) + 1
    chunk = text[start:start + 600]
    chunk = chunk[: chunk.rfind("\n") + 1]
    if "-----" not in chunk and len(chunk) > 400:
        out.append(chunk.rstrip("\n"))
open(sys.argv[1], "w").write("\n-----\n".join(out) + "\n")
print(len(out), "prefixes")
