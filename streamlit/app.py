"""Dashboard do pipeline CDC + Medalhão sobre a API do Hacker News."""

import os

import pandas as pd
import psycopg2
import psycopg2.extensions
import streamlit as st

# NUMERIC do Postgres chega como Decimal, e o Altair (que desenha os gráficos
# do Streamlit) não reconhece esse tipo: trata a coluna como categoria e o
# eixo sai errado. Converter para float na saída do driver resolve de uma vez,
# em vez de lembrar de converter coluna a coluna em cada painel.
psycopg2.extensions.register_type(
    psycopg2.extensions.new_type(
        psycopg2.extensions.DECIMAL.values,
        "DEC2FLOAT",
        lambda value, cursor: float(value) if value is not None else None,
    )
)

st.set_page_config(page_title="Hacker News Pipeline", layout="wide")


def _connect():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "postgres-hackernews"),
        port=os.getenv("WAREHOUSE_PORT", "5432"),
        dbname=os.getenv("WAREHOUSE_DB", "hackernewsdb"),
        user=os.getenv("WAREHOUSE_USER", "hackernews"),
        password=os.getenv("WAREHOUSE_PASSWORD", "hackernews123"),
    )


@st.cache_data(ttl=30)
def run_query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """
    Monta o DataFrame pelo cursor em vez de usar pd.read_sql, que reclama de
    conexão psycopg2 crua com um UserWarning. De brinde, os parâmetros vão
    pelo psycopg2 -- nada de valor interpolado em string de SQL.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            columns = [c.name for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
        finally:
            cur.close()
    finally:
        conn.close()


def load(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """
    Mesma consulta, mas com a falha contida no painel: em vez de derrubar a
    página inteira com traceback, mostra o erro ali e devolve vazio para o
    resto do dashboard continuar carregando.
    """
    try:
        return run_query(sql, params)
    except psycopg2.Error as exc:
        st.error(f"Falha ao consultar o warehouse: {exc}")
        return pd.DataFrame()


def main() -> None:
    """
    O dashboard inteiro vive aqui para poder sair cedo com `return` nos dois
    casos em que não há o que desenhar: banco fora do ar e pipeline que ainda
    não rodou. O st.stop() faria o mesmo, mas só funciona dentro do runtime do
    Streamlit -- com return, rodar o arquivo com python direto testa o fluxo.
    """
    st.title("Hacker News Pipeline")
    st.caption("CDC + Arquitetura Medalhão | Apache Airflow + PostgreSQL")

    if st.button("Atualizar dados"):
        st.cache_data.clear()

    # Sem banco no ar não existe painel nenhum para mostrar, então a checagem vem
    # antes de tudo: uma mensagem clara é melhor que sete tracebacks empilhados.
    try:
        run_query("SELECT 1")
    except psycopg2.Error as exc:
        st.error(
            "Não consegui conectar ao warehouse. Confira se os containers estão "
            "no ar com `docker compose ps`."
        )
        st.caption(f"Detalhe técnico: {exc}")
        return

    # --- painel de execuções -----------------------------------------------

    st.header("Execuções da DAG")

    runs = load(
        """
        SELECT run_id, status, started_at, finished_at,
               ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 1) AS duration_s,
               ids_discovered, items_fetched, bronze_inserted, silver_upserted
        FROM control.run_log
        ORDER BY started_at DESC
        LIMIT 20
        """
    )

    if runs.empty:
        st.info(
            "O pipeline ainda não rodou nenhuma vez. Abra o Airflow em "
            "localhost:8080, ative a DAG `hackernews_pipeline` e dispare uma "
            "execução — os painéis aparecem aqui em seguida."
        )
        return

    layers = load("SELECT layer, row_count FROM control.v_layer_counts")

    latest = runs.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Última execução", latest["status"])
    col2.metric("Candidatos descobertos", int(latest["ids_discovered"] or 0))
    col3.metric("Versões novas no Bronze", int(latest["bronze_inserted"] or 0))
    col4.metric("Stories atualizadas no Silver", int(latest["silver_upserted"] or 0))

    # Quatro por linha: são 8 camadas, e uma coluna por camada deixaria cada
    # rótulo espremido em um oitavo da tela.
    layer_rows = layers.to_dict("records")
    for start in range(0, len(layer_rows), 4):
        cols = st.columns(4)
        for col, row in zip(cols, layer_rows[start:start + 4]):
            col.metric(row["layer"], int(row["row_count"]))

    st.dataframe(runs, use_container_width=True, hide_index=True)

    st.divider()

    # --- ranking atual -------------------------------------------------------

    st.header("Ranking atual")

    ranking = load(
        """
        SELECT rank_position, title, domain, author, score, descendants,
               age_hours, score_per_hour, url
        FROM gold.current_ranking
        ORDER BY rank_position
        LIMIT 25
        """
    )

    if ranking.empty:
        st.info("A tabela gold.current_ranking ainda está vazia.")
    else:
        st.dataframe(
            ranking,
            use_container_width=True,
            hide_index=True,
            column_config={"url": st.column_config.LinkColumn("url")},
        )

    st.divider()

    # --- evolução de score -----------------------------------------------------

    st.header("Evolução de score (a prova do CDC)")
    st.caption(
        "A API do Hacker News só devolve o estado atual de um post. Esse histórico "
        "de versões é reconstruído a partir do Bronze, que acumula uma linha por "
        "mudança real detectada via hash do payload."
    )

    top_moved = load(
        """
        SELECT item_id, title, MAX(version_number) AS versions,
               MAX(score) - MIN(score) AS score_growth
        FROM gold.score_velocity
        GROUP BY item_id, title
        ORDER BY score_growth DESC NULLS LAST
        LIMIT 10
        """
    )

    if not top_moved.empty:
        # A opção do selectbox é o item_id, e não o título, com o título entrando
        # só na exibição. Escolher pelo título quebrava em dois casos reais: item
        # moderado fica com título NULL e a busca de volta não achava a linha
        # (IndexError), e dois posts com o mesmo título sempre caíam no primeiro.
        titles = dict(zip(top_moved["item_id"], top_moved["title"]))
        selected_id = st.selectbox(
            "Story",
            [int(i) for i in top_moved["item_id"]],
            format_func=lambda i: titles.get(i) or f"(sem título) #{i}",
        )

        velocity = load(
            """
            SELECT version_number, captured_at, score, descendants, score_delta,
                   minutes_since_previous
            FROM gold.score_velocity
            WHERE item_id = %s
            ORDER BY version_number
            """,
            (selected_id,),
        )
        if not velocity.empty:
            c1, c2 = st.columns(2)
            c1.line_chart(velocity.set_index("captured_at")[["score"]])
            c2.line_chart(velocity.set_index("captured_at")[["descendants"]])
            st.dataframe(velocity, use_container_width=True, hide_index=True)
    else:
        st.info(
            "Ainda não há story com mais de uma versão capturada. "
            "Aguarde a próxima execução."
        )

    st.divider()

    # --- top domínios ----------------------------------------------------------

    st.header("Top domínios")

    domains = load(
        """
        SELECT domain, total_stories, avg_score, median_score, max_score, total_comments
        FROM gold.top_domains
        ORDER BY total_stories DESC
        LIMIT 20
        """
    )

    if not domains.empty:
        c1, c2 = st.columns([2, 1])
        c1.bar_chart(domains.set_index("domain")["total_stories"])
        c2.dataframe(domains, use_container_width=True, hide_index=True)
    else:
        st.info("A tabela gold.top_domains ainda está vazia.")

    st.divider()

    # --- atividade por hora ------------------------------------------------------

    st.header("Atividade por hora de publicação")
    st.caption(
        "Stories publicadas nos últimos 7 dias, agrupadas pela hora de publicação "
        "na origem."
    )

    hourly = load(
        """
        SELECT hour_bucket, total_stories, avg_score, total_comments
        FROM gold.hourly_activity
        ORDER BY hour_bucket
        """
    )

    if not hourly.empty:
        c1, c2 = st.columns(2)
        c1.bar_chart(hourly.set_index("hour_bucket")["total_stories"])
        c2.line_chart(hourly.set_index("hour_bucket")["avg_score"])
    else:
        st.info("A tabela gold.hourly_activity ainda está vazia.")

    st.divider()

    # --- moderação ---------------------------------------------------------------

    st.header("Moderação (soft delete)")
    st.caption(
        "Stories que a moderação marcou como dead/deleted — capturadas porque o "
        "Bronze guarda a versão anterior à moderação, algo que a API não expõe."
    )

    moderation = load(
        """
        SELECT item_id, title, author, domain, final_state,
               first_seen_at, flagged_at, minutes_until_flagged, score_when_flagged
        FROM gold.moderation_stats
        ORDER BY flagged_at DESC
        LIMIT 100
        """
    )

    if not moderation.empty:
        st.dataframe(moderation, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma story moderada foi capturada ainda nesta janela.")


main()
