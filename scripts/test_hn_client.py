import logging
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))

import hn_client as hn
from hashing import payload_hash

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def main() -> int:
    new_ids = hn.fetch_id_list("newstories", limit=500)
    top_ids = hn.fetch_id_list("topstories", limit=200)
    upd_ids = hn.fetch_updated_ids()

    candidates = sorted(set(new_ids) | set(top_ids) | set(upd_ids))
    print(f"\ncandidatos únicos: {len(candidates)}")

    items = hn.fetch_items(candidates)
    print(f"itens obtidos:     {len(items)}")

    types = Counter(i.get("type") for i in items)
    print(f"\ntipos: {dict(types)}")
    print(f"sem campo 'time':  {sum(1 for i in items if 'time' not in i)}")
    print(f"dead:              {sum(1 for i in items if i.get('dead'))}")
    print(f"deleted:           {sum(1 for i in items if i.get('deleted'))}")

    stories = [i for i in items if i.get("type") in ("story", "job") and "time" in i]
    print(f"\nstories/jobs válidos: {len(stories)}")

    if stories:
        s = stories[0]
        print(f"\nexemplo: {s.get('title')!r}")
        print(f"  score={s.get('score')} comments={s.get('descendants')} url={s.get('url')}")
        print(f"  hash={payload_hash(s)}")

    # O mesmo payload tem que dar o mesmo hash nas duas chamadas
    assert all(payload_hash(s) == payload_hash(dict(s)) for s in stories[:50])
    print("\nhash estável — OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
