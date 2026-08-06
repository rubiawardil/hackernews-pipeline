from __future__ import annotations

import logging

from airflow.models import BaseOperator

log = logging.getLogger(__name__)

REQUIRED_FIELDS = ("id", "type", "time")
VALID_TYPES = ("story", "job")


class ValidateItemsOperator(BaseOperator):
    """
    Filtra os itens da API antes de irem para o Bronze.
    Descarta o que não é story/job e o que não tem id, type e time.
    """

    def __init__(self, *, items_task_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.items_task_id = items_task_id

    def execute(self, context) -> list[dict]:
        entries = context["ti"].xcom_pull(task_ids=self.items_task_id) or []

        valid: list[dict] = []
        dropped_type = 0
        dropped_fields = 0

        for entry in entries:
            item = entry.get("item") if isinstance(entry, dict) else None
            if not isinstance(item, dict):
                dropped_fields += 1
                continue
            if item.get("type") not in VALID_TYPES:
                dropped_type += 1
                continue
            if not all(field in item for field in REQUIRED_FIELDS):
                dropped_fields += 1
                continue
            valid.append(entry)

        log.info(
            "validate_items: %d válidos, %d descartados por tipo, %d descartados por campo ausente (de %d recebidos)",
            len(valid), dropped_type, dropped_fields, len(entries),
        )

        if not valid:
            raise ValueError("nenhum item válido após validação -- possível problema na API ou mudança de schema")

        return valid
