"""OData v3 query parser for $filter, $select, $orderby, $top, $skip."""
import re
from datetime import datetime
from uuid import UUID
from sqlalchemy import Table, and_, or_, cast, String

_TOKEN_RE = re.compile(
    r"datetime'([^']*)'"
    r"|guid'([^']*)'"
    r"|'([^']*)'"
    r"|(null)\b"
    r"|(true|false)\b"
    r"|(-?\d+\.\d+)"
    r"|(-?\d+)"
    r"|\b(eq|ne|gt|ge|lt|le)\b"
    r"|\b(and|or)\b"
    r"|\b(not)\b"
    r"|\b(substringof|startswith|endswith|contains)\b"
    r"|([A-Za-z\u0400-\u04FF_]\w*)"
    r"|(\()"
    r"|(\))"
    r"|(,)",
    re.UNICODE,
)

_NAMES = ["DATETIME","GUID","STRING","NULL","BOOLEAN","DECIMAL","INTEGER",
          "CMP","LOGIC","NOT","FUNC","IDENT","LPAREN","RPAREN","COMMA"]

_OPS = {"eq":"__eq__","ne":"__ne__","gt":"__gt__","ge":"__ge__","lt":"__lt__","le":"__le__"}


def _tokenize(text):
    tokens = []
    for m in _TOKEN_RE.finditer(text):
        for i, name in enumerate(_NAMES):
            val = m.group(i + 1)
            if val is not None:
                tokens.append((name, val))
                break
    return tokens


def _parse_val(tt, tv):
    if tt == "DATETIME":
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(tv, fmt)
            except ValueError:
                continue
        return tv
    if tt == "GUID":
        return UUID(tv)
    if tt == "STRING":
        return tv
    if tt == "BOOLEAN":
        return tv.lower() == "true"
    if tt == "INTEGER":
        return int(tv)
    if tt == "DECIMAL":
        return float(tv)
    if tt == "NULL":
        return None
    return tv


class _Parser:
    def __init__(self, tokens, table):
        self.t = tokens
        self.p = 0
        self.table = table

    def peek(self):
        return self.t[self.p] if self.p < len(self.t) else None

    def eat(self, expected=None):
        tok = self.t[self.p]
        if expected and tok[0] != expected:
            raise ValueError(f"Expected {expected}, got {tok}")
        self.p += 1
        return tok

    def expr(self):
        return self.or_expr()

    def or_expr(self):
        left = self.and_expr()
        while self.peek() and self.peek()[0] == "LOGIC" and self.peek()[1] == "or":
            self.eat()
            left = or_(left, self.and_expr())
        return left

    def and_expr(self):
        left = self.cmp_expr()
        while self.peek() and self.peek()[0] == "LOGIC" and self.peek()[1] == "and":
            self.eat()
            left = and_(left, self.cmp_expr())
        return left

    def cmp_expr(self):
        tok = self.peek()
        if tok is None:
            raise ValueError("Unexpected end")
        if tok[0] == "LPAREN":
            self.eat()
            e = self.expr()
            self.eat("RPAREN")
            return e
        if tok[0] == "NOT":
            self.eat()
            return ~self.cmp_expr()
        if tok[0] == "FUNC":
            return self.func_expr()
        field = self.eat("IDENT")
        col = self.table.c[field[1]]
        op = self.eat("CMP")
        vt = self.eat()
        val = _parse_val(vt[0], vt[1])
        if val is None:
            return col.is_(None) if op[1] == "eq" else col.isnot(None)
        return getattr(col, _OPS[op[1]])(val)

    def func_expr(self):
        fn = self.eat("FUNC")
        self.eat("LPAREN")
        args = [self.eat()]
        while self.peek() and self.peek()[0] == "COMMA":
            self.eat()
            args.append(self.eat())
        self.eat("RPAREN")
        name = fn[1].lower()
        if name in ("substringof", "contains"):
            sv = _parse_val(args[0][0], args[0][1])
            col = self.table.c[args[1][1]] if len(args) > 1 else self.table.c[args[0][1]]
            return cast(col, String).ilike(f"%{sv}%")
        if name == "startswith":
            col = self.table.c[args[0][1]]
            sv = _parse_val(args[1][0], args[1][1])
            return cast(col, String).ilike(f"{sv}%")
        if name == "endswith":
            col = self.table.c[args[0][1]]
            sv = _parse_val(args[1][0], args[1][1])
            return cast(col, String).ilike(f"%{sv}")
        raise ValueError(f"Unknown function: {name}")


def parse_filter(filter_str, table):
    if not filter_str or not filter_str.strip():
        return None
    tokens = _tokenize(filter_str)
    if not tokens:
        return None
    return _Parser(tokens, table).expr()


def parse_select(select_str, table):
    if not select_str:
        return None
    cols = [table.c[n.strip()] for n in select_str.split(",") if n.strip() in table.c]
    return cols if cols else None


def parse_orderby(orderby_str, table):
    if not orderby_str:
        return None
    clauses = []
    for part in orderby_str.split(","):
        part = part.strip()
        if not part:
            continue
        pieces = part.split()
        fn = pieces[0]
        d = pieces[1].lower() if len(pieces) > 1 else "asc"
        if fn in table.c:
            col = table.c[fn]
            clauses.append(col.desc() if d == "desc" else col.asc())
    return clauses if clauses else None
