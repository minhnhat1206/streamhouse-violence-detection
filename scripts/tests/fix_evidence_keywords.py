"""Fix false-positive evidence detection: 'anh' in 'canh', 'anh' in 'canh bao'"""
with open("/home/user/streamhouse/scripts/chatbot/agent.py", "r", encoding="utf-8") as f:
    code = f.read()

# Remove standalone "anh" and "hinh" from substring keywords (false positive in "canh"/"hinh-thuoc")
code = code.replace('"anh", "hinh", ', '"hinh anh", ')
# Also remove "anh" if it's present differently
import re
code = re.sub(r'"ảnh", "hình", "hinh", ', '"hình ảnh", "hinh anh", ', code)

# Move to word-boundary tokens
code = code.replace(
    '_EVIDENCE_WORD_TOKENS = ("anh",)',
    '_EVIDENCE_WORD_TOKENS = ("anh", "anh", "hinh")  # word-boundary prevents false-pos'
)

# Simpler approach: just ensure "anh" in KEYWORDS doesn't exist as standalone
# The real fix: remove "anh" entry that causes cảnh→False positive
# Check what's actually in the file
import re as _re
m = _re.search(r'_EVIDENCE_KEYWORDS = \((.*?)\)', code, _re.DOTALL)
if m:
    print("Current EVIDENCE_KEYWORDS block:")
    print(m.group(0)[:300])

m2 = _re.search(r'_EVIDENCE_WORD_TOKENS = .*', code)
if m2:
    print("Current WORD_TOKENS:")
    print(m2.group(0))

import ast
try:
    ast.parse(code)
    print("Syntax OK")
except SyntaxError as e:
    print(f"SYNTAX ERROR: {e}")
