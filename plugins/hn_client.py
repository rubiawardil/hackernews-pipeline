from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

BASE_URL = "https://hacker-news.firebaseio.com/v0"
TIMEOUT = 15
MAX_WORKERS = 10


def _build_session() -> requests.Session:
    # Este retry cobre a falha transitória de uma requisição; o do Airflow
    # cobre a task inteira cair.
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=MAX_WORKERS))
    session.headers.update({"User-Agent": "hackernews-pipeline/1.0"})
    return session


def fetch_id_list(endpoint: str, limit: int | None = None) -> list[int]:
    """Lê newstories, topstories ou beststories e devolve os ids."""
    session = _build_session()
    try:
        response = session.get(f"{BASE_URL}/{endpoint}.json", timeout=TIMEOUT)
        response.raise_for_status()
        ids = response.json() or []
    finally:
        session.close()

    if limit is not None:
        ids = ids[:limit]

    log.info("%s: %d ids", endpoint, len(ids))
    return [int(i) for i in ids]


def fetch_updated_ids() -> list[int]:
    """updates.json devolve {"items": [...], "profiles": [...]} — só items importa."""
    session = _build_session()
    try:
        response = session.get(f"{BASE_URL}/updates.json", timeout=TIMEOUT)
        response.raise_for_status()
        payload = response.json() or {}
    finally:
        session.close()

    ids = payload.get("items") or []
    log.info("updates: %d ids alterados", len(ids))
    return [int(i) for i in ids]


def _fetch_one(session: requests.Session, item_id: int) -> dict | None:
    response = session.get(f"{BASE_URL}/item/{item_id}.json", timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_items(item_ids: list[int], max_workers: int = MAX_WORKERS) -> list[dict]:
    """
    Busca os detalhes em paralelo. Id que falha ou vem nulo sai com log --
    perder um item não pode derrubar a execução inteira.
    """
    if not item_ids:
        return []

    items: list[dict] = []
    failures = 0
    session = _build_session()

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_fetch_one, session, i): i for i in item_ids}
            for future in as_completed(futures):
                item_id = futures[future]
                try:
                    item = future.result()
                except Exception as exc:
                    failures += 1
                    log.warning("falha ao buscar item %s: %s", item_id, exc)
                    continue
                if item:
                    items.append(item)
    finally:
        session.close()

    log.info("%d itens obtidos de %d ids (%d falhas)", len(items), len(item_ids), failures)
    return items
