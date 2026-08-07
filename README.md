# hackernews-pipeline

Pipeline de dados com CDC e arquitetura Medalhão sobre a API pública do Hacker News, orquestrado com Apache Airflow.

**Stack:** Apache Airflow 2.9.3 · PostgreSQL 16 · Streamlit · Docker Compose

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

[WIP]

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

[WIP]

## Decisões técnicas

[WIP]

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

