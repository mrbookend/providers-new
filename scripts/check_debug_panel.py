#!/usr/bin/env python3
import re
import sys
from pathlib import Path

s = Path("app_admin.py").read_text(encoding="utf-8")
lines = s.splitlines()

m = re.search(r'(?m)^([ \t]*)def\s+__HCR_debug_panel\(\):\s*$', s)
if not m:
    print("ERROR: __HCR_debug_panel() missing")
    sys.exit(1)

def_ln = s[: m.start()].count("\n") + 1
base = len(m.group(1).expandtabs(4))


def indent_len(t: str) -> int:
    return len(re.match(r'^([ \t]*)', t).group(1).expandtabs(4))


# find function end by dedent
end_ln = len(lines)
for i in range(def_ln, len(lines)):
    t = lines[i]
    if t.strip() == "":
        continue
    if indent_len(t) <= base:
        end_ln = i + 1
        break

TITLES = {
    "DB quick probes",
    "Index parity",
    "Index maintenance",
    "Index maintenance — drop legacy",
    "Index parity (diagnostic only)",
    "Index maintenance — drop legacy vendor indexes",
}
exp_re = re.compile(r'^\s*with\s+st\.expander\(\s*([\'"])(.*?)\1')

viol = []
for idx, ln in enumerate(lines, start=1):
    m2 = exp_re.match(ln)
    if not m2:
        continue
    title = m2.group(2)
    if title in TITLES and not (def_ln <= idx < end_ln):
        viol.append((idx, title))

dbg_blocks = [
    i + 1 for i, ln in enumerate(lines) if re.match(r'^\s*with\s+_tabs\[\s*5\s*\]\s*:\s*$', ln)
]
guard_calls = [i + 1 for i, ln in enumerate(lines) if 'globals().get("__HCR_debug_panel"' in ln]

ok = True
if len(dbg_blocks) != 1:
    print(
        f"ERROR: expected 1 'with _tabs[5]:' block, found {len(dbg_blocks)} at lines {dbg_blocks}"
    )
    ok = False
if len(guard_calls) != 1:
    print(
        f"ERROR: expected 1 guarded call to __HCR_debug_panel(), found {len(guard_calls)} at lines {guard_calls}"
    )
    ok = False
if viol:
    print("ERROR: expander(s) outside __HCR_debug_panel():")
    for n, t in viol:
        print(f"  line {n}: {t}")
    ok = False

if ok:
    print("OK: All debug expanders are inside __HCR_debug_panel(); one guarded Debug call present.")
sys.exit(0 if ok else 1)
