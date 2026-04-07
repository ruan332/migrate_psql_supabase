# PostgreSQL → Supabase Migration Tool

Script Python que realiza migração completa de um banco PostgreSQL para o Supabase — incluindo **schema** (tabelas, constraints, indexes, views, functions, triggers) e **dados** — via conexão direta PostgreSQL.

---

## Funcionalidades

- Mapeia o banco de origem por completo via `pg_catalog` e `information_schema`
- Cria todo o schema no Supabase na ordem correta (topological sort de FK)
- Copia dados em batches com progress bar interativa no terminal
- Trunca o destino antes de inserir (migração limpa)
- Valida a contagem de linhas por tabela ao final
- Salva log estruturado em arquivo `migration_YYYYMMDD_HHMMSS.log`
- Erros não-fatais não interrompem a migração — são exibidos no relatório final
- Retry automático de conexão (3 tentativas com backoff exponencial)

### Objetos migrados

| Objeto                  | Suportado |
|-------------------------|:---------:|
| Schemas customizados    | ✅ |
| Extensions              | ✅ |
| Sequences               | ✅ |
| Tabelas + colunas       | ✅ |
| Primary Keys            | ✅ |
| Foreign Keys            | ✅ |
| Unique Constraints      | ✅ |
| Check Constraints       | ✅ |
| Indexes                 | ✅ |
| Views                   | ✅ |
| Materialized Views      | ✅ |
| Functions / Procedures  | ✅ |
| Triggers                | ✅ |
| Dados (todos os registros) | ✅ |

---

## Pré-requisitos

- Python 3.10+
- Acesso de rede ao banco PostgreSQL de origem
- Acesso de rede ao Supabase de destino (porta 5432 ou 5433)

---

## Instalação

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/migration_supabase.git
cd migration_supabase

# Instale as dependências
pip install -r requirements.txt
```

---

## Configuração

### Opção 1 — Arquivo `.env` (recomendado)

Copie o template e preencha com suas credenciais:

```bash
cp .env.example .env
```

Edite o `.env`:

```env
# ── Banco de origem — PostgreSQL ──────────────────────
PG_HOST=192.168.1.100
PG_PORT=5432
PG_DBNAME=nome_do_banco
PG_USER=postgres
PG_PASSWORD=sua_senha

# ── Banco de destino — Supabase ───────────────────────
# Encontre em: Dashboard > Project Settings > Database
SUPA_HOST=db.xxxxxxxxxxxx.supabase.co
SUPA_PORT=5432
SUPA_DBNAME=postgres
SUPA_USER=postgres
SUPA_PASSWORD=sua_senha_supabase

# ── Configurações opcionais ───────────────────────────
BATCH_SIZE=1000
EXCLUDE_SCHEMAS=pg_temp,pg_toast
EXCLUDE_TABLES=
LOG_LEVEL=INFO
```

### Opção 2 — Interativa

Execute sem o `.env`. O script solicitará cada campo e oferecerá salvar automaticamente.

---

## Como usar

```bash
python migrate.py
```

O script executará 6 stages automaticamente:

```
STAGE 0 — Carrega credenciais (.env ou prompt interativo)
STAGE 1 — Testa conexão com ambos os bancos
STAGE 2 — Mapeia o banco de origem por completo
STAGE 3 — Aplica schema no Supabase (DDL)
STAGE 4 — Migra dados em batches com progress bar
STAGE 5 — Valida contagem de linhas por tabela
STAGE 6 — Exibe relatório final e salva o log
```

---

## Credenciais do Supabase

As credenciais de conexão direta estão em:

> **Supabase Dashboard** → **Project Settings** → **Database** → **Connection parameters**

| Campo          | Onde encontrar |
|----------------|----------------|
| `SUPA_HOST`    | Host (ex: `db.abcdefgh.supabase.co`) |
| `SUPA_PORT`    | Port (geralmente `5432`) |
| `SUPA_DBNAME`  | Database name (`postgres`) |
| `SUPA_USER`    | User (`postgres`) |
| `SUPA_PASSWORD`| Database password (definida na criação do projeto) |

---

## Configurações opcionais

| Variável          | Padrão                    | Descrição |
|-------------------|---------------------------|-----------|
| `BATCH_SIZE`      | `1000`                    | Linhas por batch de inserção |
| `EXCLUDE_SCHEMAS` | `pg_temp,pg_toast`        | Schemas a ignorar (separados por vírgula) |
| `EXCLUDE_TABLES`  | *(vazio)*                 | Tabelas a ignorar — aceita `schema.tabela` ou só `tabela` |
| `LOG_LEVEL`       | `INFO`                    | Nível de log: `INFO` ou `DEBUG` |

---

## Exemplo de saída

```
╭─────────────────────────────────────────────────────────────╮
│  PostgreSQL → Supabase Migration Tool                       │
╰─────────────────────────────────────────────────────────────╯

STAGE 1 — Testando Conexões
✅  Origem conectada  → PostgreSQL 16.4  (DB: meu_banco)
✅  Destino conectado → PostgreSQL 17.6  (DB: postgres)

STAGE 2 — Mapeando Banco de Origem
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━┓
┃ Objeto               ┃ Qtd ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━┩
│ Tabelas              │  24 │
│ Foreign Keys         │  18 │
│ Views                │   5 │
│ ...                  │ ... │
└──────────────────────┴─────┘

STAGE 4 — Migrando Dados
  ↳ usuarios    ━━━━━━━━━━━━━━━━━━━━━━ 10432/10432   0:00:08
  ↳ pedidos     ━━━━━━━━━━━━━━━━━━━━━━ 85201/85201   0:01:12

STAGE 6 — Relatório Final
╭─────────────────────────────────────╮
│  SUCESSO COMPLETO ✅                 │
│  Duração total:  6m 18s             │
│  Tabelas migradas: 24               │
│  Erros de schema:  0                │
│  Erros de dados:   0                │
│  Divergências:     0                │
╰─────────────────────────────────────╯
```

---

## Arquivos do projeto

```
migration_supabase/
├── migrate.py          # Script principal
├── .env.example        # Template de credenciais
├── .env                # Suas credenciais (não versionado)
├── requirements.txt    # Dependências Python
├── .gitignore          # Protege .env e logs
└── README.md           # Esta documentação
```

---

## Dependências

| Pacote              | Versão   | Uso |
|---------------------|----------|-----|
| `psycopg2-binary`   | ≥ 2.9.10 | Conexão PostgreSQL |
| `python-dotenv`     | 1.0.1    | Leitura do arquivo `.env` |
| `rich`              | 13.7.1   | Terminal colorido, progress bar, tabelas |

---

## Segurança

- O arquivo `.env` está no `.gitignore` e **nunca é versionado**
- Senhas são solicitadas via `getpass` (sem eco no terminal)
- Nenhuma credencial é exibida nos logs

---

## Limitações conhecidas

- Tipos customizados (`ENUM`, domínios) precisam ser criados manualmente no Supabase antes de rodar a migração, pois o script não replica `CREATE TYPE`
- O Supabase bloqueia algumas extensions por política de segurança — erros dessas extensions são não-fatais e o script continua
- Funções que referenciam objetos de outros schemas podem falhar se esses schemas não existirem no destino

---

## Licença

MIT
