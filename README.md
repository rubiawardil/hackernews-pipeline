# hackernews-pipeline

Pipeline de dados com CDC e arquitetura Medalhão sobre a API pública do Hacker News, orquestrado com Apache Airflow.

**Stack:** Apache Airflow 2.9.3 · PostgreSQL 16 · Streamlit · Docker Compose

## Apresentação em vídeo

Vídeo disponível no link: https://drive.google.com/drive/folders/1aR4KcDQpE70QlK_6g7R70NtneRSeCuwC?usp=drive_link

## O Hacker News e o problema

### O que é o Hacker News

O [Hacker News](https://news.ycombinator.com) é um agregador de links da comunidade de tecnologia, mantido desde 2007 pela Y Combinator, aceleradora de startups do Vale do Silício. O funcionamento é simples: qualquer pessoa publica um link ou uma pergunta, a comunidade vota, e os posts com mais votos sobem para a primeira página. Na prática, é um termômetro do que o meio técnico está lendo e discutindo hoje.

Todo conteúdo do site é exposto por uma **API pública, gratuita e sem autenticação**, hospedada no Firebase. Não é preciso chave, cadastro nem OAuth, o que significa que qualquer pessoa consegue clonar este repositório e rodar o pipeline sem configurar credencial nenhuma. Foi um dos motivos de termos escolhido essa fonte.

A API trata tudo como *item*, e cada item tem um `type`. Os campos que importam para este projeto:

| Campo              | O que é                                                                                |
| ------------------ | -------------------------------------------------------------------------------------- |
| `type`             | `story` (um link postado), `job` (vaga divulgada pela Y Combinator), `comment`, `poll` |
| `score`            | Pontuação, resultado dos votos da comunidade                                           |
| `descendants`      | Número total de comentários no post                                                    |
| `by`               | Usuário que publicou                                                                   |
| `time`             | Data de publicação                                                                     |
| `url`              | Link externo apontado pelo post                                                        |
| `dead` / `deleted` | Flags de moderação (o post foi derrubado ou removido)                                  |

Os endpoints usados pelo pipeline, todos sob `https://hacker-news.firebaseio.com/v0`:

- `newstories.json` — ids dos posts mais recentes
- `topstories.json` — ids do ranking atual
- `updates.json` — ids alterados recentemente
- `item/{id}.json` — o detalhe de um item

### O problema

Um post do Hacker News não é um dado estático. Depois de publicado, o `score` continua subindo conforme os votos chegam, o número de comentários cresce, o título às vezes é editado, e a moderação pode derrubar o post marcando-o como `dead` ou `deleted`.

Se a gente simplesmente baixasse a lista de posts a cada execução e gravasse tudo de novo, teríamos dois problemas: Primeiro, reprocessaríamos milhares de registros que não mudaram nada. Segundo, perderíamos a informação mais interessante que existe ali: **como cada post evoluiu ao longo do tempo**. A API só devolve o estado atual, ela não tem endpoint de histórico.

A API do Hacker News também **não tem campo de "última modificação"**. Não dá para saber o que mudou desde a última vez da forma convencional. A detecção de mudança precisa ser feita do lado de quem consome.

O pipeline resolve isso com CDC: descobre o que mudou, grava só o que é novo, e no caminho constrói um histórico de versões que não existe na origem.

## O que o pipeline faz

A DAG roda a cada 15 minutos e executa 16 tasks. Em linhas gerais:

1. **Abre o registro da execução** em `control.run_log`, para deixar rastro do que aconteceu.
2. **Descobre candidatos** consultando três endpoints da API em paralelo: `newstories` (500 mais recentes), `topstories` (200 do ranking) e `updates.json` (o que mudou recentemente). As três listas viram um conjunto único, sem repetição, em média dá entre 640 e 690 IDs por execução.
3. **Busca o detalhe de cada item** com um `ThreadPoolExecutor` de 10 threads. Item que falha individualmente é registrado no log e descartado, sem derrubar a execução.
4. **Valida os itens** num operator próprio, que descarta o que não é story/job e o que não tem os campos mínimos.
5. **Grava no Bronze** só as versões que realmente mudaram, comparando por hash do payload.
6. **Atualiza o Silver** com o estado atual de cada item, um registro por post.
7. **Reconstrói as cinco tabelas do Gold** em paralelo.
8. **Roda cinco checagens de qualidade** e grava o resultado em `control.dq_results`. Se alguma falhar, a execução falha.
9. **Fecha o registro da execução** com as contagens finais e o status.

## Arquitetura

A organização segue a arquitetura Medalhão, com um schema por camada no PostgreSQL.

### Camada Bronze — `bronze.item_versions`

Guarda o payload JSON bruto, exatamente como veio da API, sem nenhuma transformação. A chave primária é o par `(item_id, payload_hash)`, e a escrita usa `INSERT ... ON CONFLICT DO NOTHING`.

Se um item não mudou desde a última execução, o hash calculado é idêntico ao que já está gravado, o conflito acontece e nada é escrito. Se o item mudou, o hash muda, uma linha nova entra e **a versão anterior continua ali**. É assim que o histórico se forma.

O `RETURNING` da query devolve só as linhas que realmente entraram, e é contando essas linhas que a task sabe quantas versões novas gravou de verdade.

### Camada Silver — `silver.stories`

Um registro por post, sempre no estado mais recente conhecido. Aqui os dados saem do JSON e viram colunas tipadas: `score` e `descendants` como inteiros, `posted_at` como timestamp, `is_dead` e `is_deleted` como booleanos, e o domínio extraído da URL por expressão regular.

A escrita é um `INSERT ... ON CONFLICT (item_id) DO UPDATE`, com uma condição que julgamos importante:

```sql
WHERE EXCLUDED.captured_at > silver.stories.captured_at
```

Sem essa cláusula, uma execução atrasada poderia sobrescrever um dado mais novo com um mais velho. É um erro clássico de pipeline incremental e custa uma linha evitá-lo.

### Camada Gold — cinco tabelas

Todas reconstruídas do zero a cada execução, com `TRUNCATE` seguido de `INSERT ... SELECT` dentro de uma transação. Reconstrução determinística é a forma mais simples de garantir idempotência nessa camada: não importa quantas vezes rodar, o resultado é função apenas do estado atual do Silver e do Bronze.


| Tabela                  | O que tem dentro                                                           |
| ----------------------- | -------------------------------------------------------------------------- |
| `gold.current_ranking`  | Top 100 posts vivos por score, com idade em horas e score por hora         |
| `gold.score_velocity`   | A trajetória de score e comentários de cada post, versão por versão        |
| `gold.top_domains`      | Os 50 domínios mais postados, com score médio, mediana e máximo            |
| `gold.hourly_activity`  | Volume de publicação e score médio por hora do dia, últimos 7 dias         |
| `gold.moderation_stats` | Posts derrubados pela moderação e quanto tempo levaram até serem flagrados |


A `score_velocity` é a que mais gostamos, porque ela só é possível por causa do CDC. A API não devolve esse histórico em lugar nenhum, ele existe apenas porque o Bronze acumulou as versões.

A `moderation_stats` tem um detalhe que só descobrimos testando contra a API de verdade: quando um post é moderado, a API **apaga** os campos do payload. Dos 32 itens `dead` que amostramos, nenhum ainda tinha `title`. Por isso essa tabela não lê a versão mais recente do post, e sim a última versão anterior à moderação, a última em que os dados ainda existiam.

### Camada Control — `control.run_log` e `control.dq_results`

Não faz parte do Medalhão, mas achamos que faltava algo para responder se a execução funcionou sem depender só da interface do Airflow. O `run_log` guarda uma linha por execução com status, duração e as contagens de cada etapa. O `dq_results` guarda o resultado de cada checagem de qualidade.

As checagens são cinco:

1. Nenhum `item_id` duplicado no Silver.
2. Nenhum post com score negativo ou data de publicação no futuro.
3. O catálogo não encolheu em relação à execução anterior.
4. Toda linha do Silver tem correspondência no Bronze.
5. A tabela `gold.current_ranking` não está vazia.

## Ferramentas e por que escolhemos cada uma

**Apache Airflow:** O pipeline é agendado por tempo, com horário fixo e dependências entre etapas que formam um grafo com ramificações e junções, três buscas em paralelo que convergem, depois cinco tabelas do Gold construídas em paralelo que convergem de novo. Esse é exatamente o cenário em que um scheduler com DAG explícita se paga. O Prefect brilha mais em fluxo dinâmico ou orientado a evento, que não é o nosso caso. Além disso, a interface do Airflow já entrega observabilidade pronta: histórico de execuções, log por task e visualização do grafo, sem precisar montar nada.

**PostgreSQL como warehouse:** O payload da API é JSON, e o tipo `JSONB` do Postgres deixa a gente guardar o dado bruto sem achatar nada e ainda consultar campo por campo. Fora isso, `ON CONFLICT` e `DISTINCT ON`, dois recursos que o pipeline usa de forma central, resolvem em SQL o que daria bastante código em Python.

**Um segundo banco, separado do metadata do Airflow:** São dois containers Postgres. O `postgres-meta` é o banco interno do Airflow; o `postgres-hackernews` é o warehouse, exposto na porta 5433. Misturar dado de negócio com o banco de controle do orquestrador é o tipo de decisão que funciona no começo, mas pode gerar problemas no futuro.

**LocalExecutor:** Dá paralelismo real entre tasks sem precisar de Redis nem de workers Celery. Para o volume deste projeto, subir uma infraestrutura distribuída seria complexidade sem retorno.

**Imagem própria via Dockerfile:** A variável de ambiente reinstala os pacotes a cada start de cada container, e a própria documentação do Airflow desaconselha usá-la fora de teste rápido. Com uma imagem construída, a instalação acontece uma vez só, as versões ficam fixadas no `requirements.txt` e versionadas no git, e os containers sobem em segundos.

**Streamlit:** É uma camada fina em cima do Gold, para mostrar que os dados chegam a algum lugar visível.

**SQL em arquivos separados, não embutido no Python:** São nove arquivos em `sql/`, numerados na ordem em que rodam. Deixar tudo inline incharia a DAG e dificultaria testar as queries direto no psql.

## Como executar

### Pré-requisitos

Docker e Docker Compose instalados (Docker Desktop já traz os dois). 

Não é preciso chave de API nem cadastro em lugar nenhum, a API do Hacker News é aberta.

### Passo a passo

**1. Clonar o repositório**

```bash
git clone <URL-DO-REPOSITORIO>
cd hackernews-pipeline
```

**2. Criar o arquivo** `.env`

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

Isso faz os containers do Airflow rodarem com o seu usuário, para conseguirem escrever na pasta `./logs`. No Linux esse passo é obrigatório, sem ele o scheduler não sobe. No macOS e no Windows não faz diferença, mas rode do mesmo jeito para não ter surpresa. Existe um `.env.example` no repositório com essa mesma instrução.

**3. Subir tudo**

```bash
docker compose up -d --build
```

Na primeira vez o `--build` é necessário, porque a imagem do Airflow precisa ser construída com as dependências. Pode levar alguns minutos. Nas próximas vezes, `docker compose up -d` basta.

O comando sobe cinco serviços: os dois bancos, o scheduler, o webserver e o Streamlit. Um sexto container, o `airflow-init`, roda uma vez só para criar as tabelas internas do Airflow, o usuário admin, a conexão com o warehouse e o pool que limita a concorrência contra a API, depois ele encerra sozinho, e ver ele como `exited` é o comportamento esperado.

**4. Esperar tudo ficar saudável**

```bash
docker compose ps
```

Espere os serviços aparecerem como `healthy`.

**5. Abrir o Airflow e ligar a DAG**

Acesse `http://localhost:8080` e entre com **admin / admin**.

A DAG `hackernews_pipeline` aparece **pausada**, clique no botão de toggle à esquerda do nome para ativá-la. Sem esse passo ela não roda nunca.

**6. Executar**

Depois de ativada, a DAG roda sozinha a cada 15 minutos. Para não esperar, dispare na hora pelo botão de play.

A primeira execução já popula todas as camadas.

### Onde ver o resultado


| O quê                | Onde                                                                                |
| -------------------- | ----------------------------------------------------------------------------------- |
| Interface do Airflow | `http://localhost:8080` (admin / admin)                                             |
| Dashboard Streamlit  | `http://localhost:8501`                                                             |
| Warehouse PostgreSQL | `localhost:5433`, banco `hackernewsdb`, usuário `hackernews`, senha `hackernews123` |


Para consultar o banco pelo terminal:

```bash
docker exec -it hackernews-warehouse-db psql -U hackernews -d hackernewsdb
```

Ou, se preferir uma visão pronta das camadas sem escrever SQL:

```bash
pip install psycopg2-binary
python scripts/inspect_warehouse.py
```

### Parar e recomeçar

```bash
docker compose stop     # desliga, preservando todos os dados
docker compose start    # liga de novo, do jeito que estava
docker compose down     # remove os containers, mas mantém os volumes
docker compose down -v  # apaga tudo, inclusive os dados do warehouse
```

O `-v` é o único que apaga dado. Use ele quando quiser testar a subida do zero.

## Como conferir que está funcionando

### Contagem por camada

Criamos uma view que resume tudo de uma vez:

```sql
SELECT * FROM control.v_layer_counts;
```

### Histórico das execuções

```sql
SELECT * FROM control.v_recent_runs;
SELECT check_name, passed, observed FROM control.dq_results
 WHERE run_id = (SELECT run_id FROM control.run_log ORDER BY started_at DESC LIMIT 1);
```

### Teste de idempotência

1. Anote os números de `control.v_layer_counts`.
2. Na interface do Airflow, abra uma execução já concluída e clique em **Clear** para reexecutá-la inteira.
3. Espere terminar e consulte a view de novo.

O `silver.stories` não ganha linha duplicada, e o Bronze só cresce se algum post realmente mudou de score entre a primeira tentativa e a segunda, o que é mudança de verdade capturada, não duplicação. A checagem de qualidade número 1, que compara total contra distintos no Silver, continua passando.

## Decisões técnicas

### Detectar mudança por hash, já que não existe timestamp na origem

Como a API não expõe "última modificação", calculamos um SHA-256 sobre um subconjunto do payload:

```python
MUTABLE_FIELDS = ("score", "title", "url", "descendants", "dead", "deleted", "text")
```

Só entram no hash os campos que mudam depois da publicação. Os demais ficam de fora de propósito: o campo `kids`, por exemplo, é a lista de IDs dos comentários e muda toda hora, mas quem já representa esse movimento é o `descendants`. Incluir `kids` faria praticamente todo item virar versão nova a cada execução, e o Bronze cresceria sem trazer informação.

O hash é ordenado por chave antes de ser calculado (`sort_keys=True`), então a ordem em que a API devolve os campos não interfere.

### Por que `ON CONFLICT` em vez de apagar e reinserir

No CDC o Bronze precisa **acumular** versões, não substituí-las. Se apagássemos as linhas da execução anterior, destruiríamos justamente o histórico que dá sentido ao projeto.

### O `DISTINCT ON` no Silver

Esse foi um bug real que só apareceu quando testamos o cenário de reexecução com atenção.

Quando uma execução é limpa e rodada de novo, ela mantém o mesmo `dag_run_id`. Se o score de algum post mudou entre a primeira tentativa e a segunda, o hash muda e o Bronze ganha uma segunda linha **com o mesmo** `dag_run_id`. Aí o `SELECT` do Silver, que filtra por `dag_run_id`, passa a devolver duas linhas para o mesmo `item_id`, e o Postgres retorna erro.

A correção foi usar `DISTINCT ON (b.item_id)` com `ORDER BY b.captured_at DESC`, ficando só com a captura mais recente de cada item.

### Retry em duas camadas

O cliente HTTP tem retry próprio, com backoff, para os códigos 429, 500, 502, 503 e 504. Isso resolve a falha momentânea de uma requisição sem envolver o orquestrador. Acima dele, o Airflow tem `retries=3` com backoff exponencial, que cobre a task inteira falhando.

Além disso, na busca de detalhes, um item que falha sozinho é registrado no log e descartado. Perder um post entre setecentos não justifica derrubar a execução.

### O `finalize_run` fecha o registro mesmo quando algo falha

A última task usa `trigger_rule="all_done"`, então ela roda independentemente do que aconteceu antes. Ela consulta o estado das outras tasks da execução, decide se o status é sucesso ou falha e grava as contagens.

Como rede de segurança adicional, o `on_failure_callback` da DAG também fecha o registro, para o caso de a execução inteira morrer antes de chegar na task de fechamento, que foi exatamente o que aconteceu na execução que estourou o timeout. Esse callback só mexe na linha se ela ainda estiver como `running`, para não sobrescrever um fechamento já feito.

### Pool para não martelar a API

As tasks que falam com a API rodam dentro de um pool com 4 slots. Somado ao `max_active_runs=1`, isso garante que nunca haja mais de uma execução ativa nem uma enxurrada de requisições simultâneas contra um serviço público e gratuito.

## Estrutura do repositório

```
hackernews-pipeline/
├── README.md                       este arquivo
├── docker-compose.yml              cinco serviços e dois volumes
├── Dockerfile                      imagem do Airflow com as dependências
├── requirements.txt                dependências com versão fixada
├── .env.example                    modelo do .env (AIRFLOW_UID)
├── dags/
│   └── hackernews_pipeline.py      a DAG, com as 16 tasks
├── plugins/
│   ├── hn_client.py                cliente HTTP com retry e paralelismo
│   ├── hashing.py                  cálculo do hash de payload
│   ├── sql_loader.py               leitura dos arquivos de sql/
│   └── validate_items_operator.py  operator próprio de validação
├── sql/
│   ├── init.sql                    schemas, tabelas, índices, views e grants
│   ├── 10_load_bronze.sql          gravação versionada no Bronze
│   ├── 20_transform_silver.sql     upsert do Silver
│   ├── 30_gold_current_ranking.sql
│   ├── 31_gold_score_velocity.sql
│   ├── 32_gold_top_domains.sql
│   ├── 33_gold_moderation_stats.sql
│   ├── 34_gold_hourly_activity.sql
│   └── 40_dq_checks.sql            as cinco checagens de qualidade
├── streamlit/
│   └── app.py                      dashboard sobre as tabelas do Gold
└── scripts/
    ├── inspect_warehouse.py        resumo das camadas pelo terminal
    └── test_hn_client.py           testa o cliente sem subir o Airflow
```
