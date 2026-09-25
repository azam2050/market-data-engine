"""توازن الأقواس واستمرار السطر — الفحص الثاني الذي كان ينقصني."""
import io, re, sys

def check(path):
    L = io.open(path, encoding="utf-8").read().split("\n")
    bad = []
    depth = 0
    opened_at = 0
    for i, l in enumerate(L, 1):
        if l.lstrip().startswith("//"):
            continue
        code = re.sub(r'"(?:[^"\\]|\\.)*"', '""', l.split("//")[0])
        if depth == 0 and code.count("(") - code.count(")") != 0:
            opened_at = i
        depth += code.count("(") - code.count(")")
        if depth < 0:
            bad.append((i, "قوس إغلاق زائد", l.strip()[:70]))
            depth = 0
            continue
        if depth > 0 and i < len(L):
            nxt = L[i]
            if nxt.strip() and not nxt.lstrip().startswith("//"):
                ind = len(nxt) - len(nxt.lstrip())
                if ind % 4 == 0:
                    bad.append((opened_at, "قوس لم يُغلق والسطر التالي ليس استمراراً", l.strip()[:70]))
                    depth = 0
    if depth != 0:
        bad.append((opened_at, f"قوس لم يُغلق حتى نهاية الملف ({depth})", ""))
    return bad

ok = True
for p in sys.argv[1:]:
    b = check(p)
    print(f"{p}: {'أقواس متوازنة ✓' if not b else str(len(b)) + ' مشكلة ✗'}")
    for i, w, l in b:
        print(f"   سطر {i}: {w}  |  {l}")
    if b:
        ok = False
sys.exit(0 if ok else 1)
