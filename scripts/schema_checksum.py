#!/usr/bin/env python3
import sys
from hashlib import sha256
from sqlite3 import connect


def main() -> None:
    db = sys.argv[1] if len(sys.argv) > 1 else "providers.db"
    con = connect(db)
    cur = con.cursor()
    cur.execute(
        "SELECT sql FROM sqlite_master WHERE type IN ('table','index','view','trigger') ORDER BY 1"
    )
    schema_parts = [(row[0] or "") for row in cur.fetchall()]
    con.close()
    digest = sha256("\n".join(schema_parts).encode()).hexdigest()
    print(digest)


if __name__ == "__main__":
    main()
