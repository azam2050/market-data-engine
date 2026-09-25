"""فحص المعرّفات غير المعرَّفة في الباين — الفحص الذي كشفه ترايدنغ فيو ولم أكن أجريه."""
import re, sys, io

BUILTIN = set("""close open high low volume time time_close hlc3 hl2 ohlc4 hlcc4 tr
bar_index last_bar_index na true false nz int float bool string color line label box table
array matrix if else for while to by and or not switch var varip type enum method export import
str math ta color line label box table array matrix map polyline
syminfo timeframe session barstate chart currency dayofweek dayofmonth year month weekofyear
hour minute second dividends earnings quandl financial splits request strategy ticker
text size location shape display scale format position order xloc yloc extend
adjustment barmerge lookahead settlement_as_close plot
in break continue simple series const input arg""".split())
ARGKEY = re.compile(r'(?:[(,]\s*)([A-Za-z_]\w*)\s*=(?!=)')


def declared(lines):
    d = set()
    for l in lines:
        if l.lstrip().startswith("//"): continue
        code = l.split("//")[0]
        m = re.match(r"\s*([A-Za-z_]\w*)\s*\(([^)]*)\)\s*=>", code)
        if m:
            d.add(m.group(1))
            for p in m.group(2).split(","):
                q = p.strip().split()
                if q: d.add(q[-1])
            continue
        m = re.match(r"\s*(?:var(?:ip)?\s+)?(?:[A-Za-z_]\w*(?:\[\])?\s+)?([A-Za-z_]\w*)\s*:?=(?!=)", code)
        if m: d.add(m.group(1))
        m = re.match(r"\s*\[([^\]]+)\]\s*=", code)
        if m:
            for p in m.group(1).split(","): d.add(p.strip())
        m = re.match(r"\s*for\s+([A-Za-z_]\w*)\s*=", code)
        if m: d.add(m.group(1))
    return d


def check(path):
    lines = io.open(path, encoding="utf-8").read().split("\n")
    d = declared(lines)
    bad = []
    for i, l in enumerate(lines, 1):
        if l.lstrip().startswith("//"): continue
        code = re.sub(r'"[^"]*"', '""', l.split("//")[0])
        code = re.sub(r"#[0-9A-Fa-f]{3,8}", "0", code)
        skip = {m.start(1) for m in ARGKEY.finditer(code)}
        for m in re.finditer(r"[A-Za-z_]\w*", code):
            n, a, b = m.group(0), m.start(), m.end()
            if a in skip: continue
            if a > 0 and code[a-1] == ".": continue          # عضو في فضاء أسماء
            if code[b:b+1] == "(": continue                   # نداء دالة
            if code[b:b+1] == "." : continue                  # فضاء أسماء
            if n in BUILTIN or n in d: continue
            bad.append((i, n, l.strip()[:70]))
    return bad


ok = True
for p in sys.argv[1:]:
    b = check(p)
    print(f"{p}: {'نظيف ✓' if not b else str(len(b)) + ' معرّف غير معرَّف ✗'}")
    for i, n, l in b[:25]:
        print(f"   سطر {i}: «{n}»   |  {l}")
    if b: ok = False
sys.exit(0 if ok else 1)
