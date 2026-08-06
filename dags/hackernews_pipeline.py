from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup

from validate_items_operator import ValidateItemsOperator

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_hackernews"
HN_API_POOL = "hn_api_pool"
NEW_STORIES_LIMIT = 500
TOP_STORIES_LIMIT = 200

ID_LIST_TIMEOUT = timedelta(minutes=2)
FETCH_ITEMS_TIMEOUT = timedelta(minutes=4)

XCOM_CANDIDATE_COUNT = "candidate_count"
XCOM_ITEM_COUNT = "item_count"


def _alert_failure(context: dict) -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    run_id = context["run_id"]
    reason = str(context.get("reason") or context.get("exception") or "dag run failed")

    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE control.run_log
                SET status = 'failed', finished_at = NOW(), error_message = %s
                WHERE run_id = %s AND status = 'running'
                """,
                (reason, run_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()
    log.error("run %s falhou: %s", run_id, reason)


def _alert_retry(context: dict) -> None:
    ti = context["task_instance"]
    log.warning("task %s da run %s vai tentar de novo (tentativa %d)", ti.task_id, context["run_id"], ti.try_number)


def _alert_success(context: dict) -> None:
    log.info("run %s concluída com sucesso", context["run_id"])


DEFAULT_ARGS = {
    "owner": "hackernews",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_retry_callback": _alert_retry,
}


def _execute_sql_file(filename: str, params: dict | None = None) -> int:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sql_loader import load_sql

    sql = load_sql(filename)
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        cur = conn.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            count = cur.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()
    return count


@dag(
    dag_id="hackernews_pipeline",
    description="CDC + Medalhão | Hacker News API -> PostgreSQL",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["hackernews", "cdc", "medallion"],
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=14),
    on_failure_callback=_alert_failure,
    on_success_callback=_alert_success,
    doc_md=__doc__,
)
def hackernews_pipeline():

    @task
    def start_run() -> None:
        from airflow.operators.python import get_current_context
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        run_id = get_current_context()["run_id"]
        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    "INSERT INTO control.run_log (run_id) VALUES (%s) ON CONFLICT (run_id) DO NOTHING",
                    (run_id,),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
        finally:
            conn.close()
        log.info("run %s aberta em control.run_log", run_id)

    @task(pool=HN_API_POOL, execution_timeout=ID_LIST_TIMEOUT)
    def fetch_new_story_ids() -> list[int]:
        import hn_client as hn

        return hn.fetch_id_list("newstories", limit=NEW_STORIES_LIMIT)

    @task(pool=HN_API_POOL, execution_timeout=ID_LIST_TIMEOUT)
    def fetch_top_story_ids() -> list[int]:
        import hn_client as hn

        return hn.fetch_id_list("topstories", limit=TOP_STORIES_LIMIT)

    @task(pool=HN_API_POOL, execution_timeout=ID_LIST_TIMEOUT)
    def fetch_updated_ids() -> list[int]:
        import hn_client as hn

        return hn.fetch_updated_ids()

    @task
    def merge_candidate_ids(new_ids: list[int], top_ids: list[int], updated_ids: list[int]) -> list[dict]:
        from airflow.operators.python import get_current_context

        new_set, top_set, updated_set = set(new_ids), set(top_ids), set(updated_ids)
        all_ids = new_set | top_set | updated_set

        def _source(item_id: int) -> str:
            if item_id in new_set:
                return "new"
            if item_id in top_set:
                return "top"
            return "updates"

        candidates = [{"id": i, "source": _source(i)} for i in sorted(all_ids)]
        log.info(
            "candidatos únicos: %d (new=%d top=%d updates=%d)",
            len(candidates), len(new_set), len(top_set), len(updated_set),
        )
        get_current_context()["task_instance"].xcom_push(
            key=XCOM_CANDIDATE_COUNT, value=len(candidates)
        )
        return candidates

    @task(pool=HN_API_POOL, execution_timeout=FETCH_ITEMS_TIMEOUT)
    def fetch_items(candidates: list[dict]) -> list[dict]:
        from airflow.operators.python import get_current_context

        import hn_client as hn

        ids = [c["id"] for c in candidates]
        source_by_id = {c["id"]: c["source"] for c in candidates}

        items = hn.fetch_items(ids)
        entries = [{"item": item, "source": source_by_id.get(item.get("id"), "updates")} for item in items]

        log.info("itens obtidos: %d de %d candidatos", len(entries), len(ids))
        get_current_context()["task_instance"].xcom_push(
            key=XCOM_ITEM_COUNT, value=len(entries)
        )
        return entries

    @task
    def load_bronze(entries: list[dict]) -> int:
        import json
        from datetime import datetime, timezone

        from airflow.operators.python import get_current_context
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from psycopg2.extras import execute_values

        from hashing import payload_hash
        from sql_loader import load_sql

        run_id = get_current_context()["run_id"]
        captured_at = datetime.now(timezone.utc)
        sql = load_sql("10_load_bronze.sql")

        rows = [
            (e["item"]["id"], payload_hash(e["item"]), json.dumps(e["item"]), e["source"], captured_at, run_id)
            for e in entries
        ]

        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        try:
            cur = conn.cursor()
            try:
                inserted = execute_values(
                    cur, sql, rows, template="(%s, %s, %s::jsonb, %s, %s, %s)", fetch=True,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
        finally:
            conn.close()

        count = len(inserted)
        log.info("bronze: %d versões novas de %d itens", count, len(entries))
        return count

    @task
    def transform_silver(bronze_inserted: int) -> int:
        from airflow.operators.python import get_current_context

        run_id = get_current_context()["run_id"]
        count = _execute_sql_file("20_transform_silver.sql", {"dag_run_id": run_id})
        log.info("silver: %d linhas upsertadas", count)
        return count

    @task
    def build_current_ranking(silver_upserted: int) -> int:
        return _execute_sql_file("30_gold_current_ranking.sql")

    @task
    def build_score_velocity(silver_upserted: int) -> int:
        return _execute_sql_file("31_gold_score_velocity.sql")

    @task
    def build_top_domains(silver_upserted: int) -> int:
        return _execute_sql_file("32_gold_top_domains.sql")

    @task
    def build_moderation_stats(silver_upserted: int) -> int:
        return _execute_sql_file("33_gold_moderation_stats.sql")

    @task
    def build_hourly_activity(silver_upserted: int) -> int:
        return _execute_sql_file("34_gold_hourly_activity.sql")

    @task
    def run_quality_checks() -> None:
        from airflow.operators.python import get_current_context
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sql_loader import load_sql

        run_id = get_current_context()["run_id"]
        sql = load_sql("40_dq_checks.sql")

        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql, {"run_id": run_id})
                conn.commit()
                cur.execute(
                    "SELECT check_name, observed FROM control.dq_results WHERE run_id = %s AND NOT passed",
                    (run_id,),
                )
                failed = cur.fetchall()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
        finally:
            conn.close()

        if failed:
            details = "; ".join(f"{name}={observed}" for name, observed in failed)
            raise AirflowFailException(f"checks de qualidade falharam: {details}")

        log.info("todos os checks de qualidade passaram")

    @task(trigger_rule="all_done")
    def finalize_run(
        candidates_task_id: str,
        items_task_id: str,
        bronze_task_id: str,
        silver_task_id: str,
    ) -> None:
        from airflow.operators.python import get_current_context
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from airflow.utils.state import TaskInstanceState

        context = get_current_context()
        run_id = context["run_id"]
        dag_run = context["dag_run"]
        ti = context["task_instance"]
        self_task_id = ti.task_id

        ids_discovered = ti.xcom_pull(task_ids=candidates_task_id, key=XCOM_CANDIDATE_COUNT) or 0
        items_fetched = ti.xcom_pull(task_ids=items_task_id, key=XCOM_ITEM_COUNT) or 0
        bronze_inserted = ti.xcom_pull(task_ids=bronze_task_id) or 0
        silver_upserted = ti.xcom_pull(task_ids=silver_task_id) or 0

        bad_states = {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}
        failed_tasks = [
            ti.task_id
            for ti in dag_run.get_task_instances()
            if ti.state in bad_states and ti.task_id != self_task_id
        ]
        status = "failed" if failed_tasks else "success"
        error_message = ("tasks com falha: " + ", ".join(failed_tasks)) if failed_tasks else None

        conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    UPDATE control.run_log
                    SET finished_at = NOW(), status = %s,
                        ids_discovered = %s, items_fetched = %s,
                        bronze_inserted = %s, silver_upserted = %s,
                        error_message = %s
                    WHERE run_id = %s
                    """,
                    (
                        status,
                        ids_discovered,
                        items_fetched,
                        bronze_inserted,
                        silver_upserted,
                        error_message,
                        run_id,
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
        finally:
            conn.close()

        log.info("run %s finalizada com status=%s", run_id, status)

    # --- grafo ---

    started = start_run()

    with TaskGroup(group_id="ingestion") as ingestion_group:
        new_ids = fetch_new_story_ids()
        top_ids = fetch_top_story_ids()
        updated_ids = fetch_updated_ids()
        candidates = merge_candidate_ids(new_ids, top_ids, updated_ids)
        raw_entries = fetch_items(candidates)

        validate_op = ValidateItemsOperator(
            task_id="validate_items",
            items_task_id=raw_entries.operator.task_id,
        )
        raw_entries >> validate_op

    started >> ingestion_group

    bronze_count = load_bronze(validate_op.output)
    silver_count = transform_silver(bronze_count)

    with TaskGroup(group_id="gold") as gold_group:
        build_current_ranking(silver_count)
        build_score_velocity(silver_count)
        build_top_domains(silver_count)
        build_moderation_stats(silver_count)
        build_hourly_activity(silver_count)

    checks = run_quality_checks()
    gold_group >> checks

    finalize = finalize_run(
        candidates_task_id=candidates.operator.task_id,
        items_task_id=raw_entries.operator.task_id,
        bronze_task_id=bronze_count.operator.task_id,
        silver_task_id=silver_count.operator.task_id,
    )
    checks >> finalize


dag_instance = hackernews_pipeline()
