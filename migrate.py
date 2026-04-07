#!/usr/bin/env python3
"""
migrate.py — PostgreSQL → Supabase Migration Tool
Realiza migração completa de schema + dados via conexão direta PostgreSQL.
"""

import os
import sys
import json
import time
import logging
import datetime
import getpass
from collections import defaultdict, deque
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import (
    Progress, SpinnerColumn, BarColumn,
    TextColumn, TimeElapsedColumn, MofNCompleteColumn,
)
from rich.logging import RichHandler

# ─────────────────────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────────────────────
console = Console()
ENV_FILE = Path(".env")
LOG_FILE = f"migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

REQUIRED_FIELDS = {
    "PG": [
        ("PG_HOST",     "Host do PostgreSQL de origem",    "localhost"),
        ("PG_PORT",     "Porta do PostgreSQL de origem",   "5432"),
        ("PG_DBNAME",   "Nome do banco de origem",         ""),
        ("PG_USER",     "Usuário do PostgreSQL de origem", "postgres"),
        ("PG_PASSWORD", "Senha do PostgreSQL de origem",   ""),
    ],
    "SUPA": [
        ("SUPA_HOST",     "Host do Supabase  (db.xxxx.supabase.co)", ""),
        ("SUPA_PORT",     "Porta do Supabase",                        "5432"),
        ("SUPA_DBNAME",   "Nome do banco Supabase",                   "postgres"),
        ("SUPA_USER",     "Usuário do Supabase",                      "postgres"),
        ("SUPA_PASSWORD", "Senha do Supabase",                        ""),
    ],
}

FK_ACTION_MAP = {
    "a": "NO ACTION",
    "r": "RESTRICT",
    "c": "CASCADE",
    "n": "SET NULL",
    "d": "SET DEFAULT",
}


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
def setup_logging(level: str = "INFO") -> logging.Logger:
    log_level = getattr(logging, level.upper(), logging.INFO)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(
        "[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    file_handler.setLevel(log_level)

    rich_handler = RichHandler(console=console, show_path=False, markup=True, rich_tracebacks=True)
    rich_handler.setLevel(log_level)

    logger = logging.getLogger("migrator")
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.addHandler(rich_handler)
    return logger


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 0 — CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────
def load_credentials() -> dict:
    load_dotenv(ENV_FILE)

    all_fields = REQUIRED_FIELDS["PG"] + REQUIRED_FIELDS["SUPA"]
    missing = [key for key, _, _ in all_fields if not os.getenv(key)]

    if missing:
        if ENV_FILE.exists():
            console.print(f"\n[yellow]⚠  Arquivo .env encontrado, mas campos ausentes: {', '.join(missing)}[/yellow]\n")
        else:
            console.print("\n[yellow]⚠  Arquivo .env não encontrado. Informe as credenciais abaixo.[/yellow]\n")

        for bank_key, fields in REQUIRED_FIELDS.items():
            label = "PostgreSQL (origem)" if bank_key == "PG" else "Supabase (destino)"
            console.print(f"[bold cyan]── {label} ──[/bold cyan]")
            for env_key, description, default in fields:
                if not os.getenv(env_key):
                    is_password = "PASSWORD" in env_key
                    if is_password:
                        value = getpass.getpass(f"  {description}: ")
                    else:
                        suffix = f" [{default}]" if default else ""
                        value = input(f"  {description}{suffix}: ").strip() or default
                    os.environ[env_key] = value
            console.print()

        save = input("💾 Salvar credenciais no arquivo .env? [S/n]: ").strip().lower()
        if save in ("", "s", "sim", "y", "yes"):
            _write_env_file()
            console.print(f"[green]✅  Credenciais salvas em {ENV_FILE}[/green]\n")

    return _collect_config()


def _write_env_file():
    lines = [
        "# ── Banco de origem — PostgreSQL ──────────────────────\n",
    ]
    for key, _, _ in REQUIRED_FIELDS["PG"]:
        lines.append(f"{key}={os.environ.get(key, '')}\n")
    lines += [
        "\n# ── Banco de destino — Supabase ───────────────────────\n",
        "# Encontre em: Dashboard > Project Settings > Database\n",
    ]
    for key, _, _ in REQUIRED_FIELDS["SUPA"]:
        lines.append(f"{key}={os.environ.get(key, '')}\n")
    lines += [
        "\n# ── Configurações opcionais ───────────────────────────\n",
        f"BATCH_SIZE={os.environ.get('BATCH_SIZE', '1000')}\n",
        f"EXCLUDE_SCHEMAS={os.environ.get('EXCLUDE_SCHEMAS', 'pg_temp,pg_toast')}\n",
        f"EXCLUDE_TABLES={os.environ.get('EXCLUDE_TABLES', '')}\n",
        f"LOG_LEVEL={os.environ.get('LOG_LEVEL', 'INFO')}\n",
    ]
    with open(ENV_FILE, "w", encoding="utf-8") as f:
        f.writelines(lines)


def _collect_config() -> dict:
    raw_schemas = os.getenv("EXCLUDE_SCHEMAS", "pg_temp,pg_toast")
    raw_tables  = os.getenv("EXCLUDE_TABLES", "")

    excluded_schemas = {s.strip() for s in raw_schemas.split(",") if s.strip()}
    excluded_schemas.update({"pg_catalog", "information_schema", "pg_toast", "pg_temp"})

    return {
        "source": {
            "host":     os.environ["PG_HOST"],
            "port":     int(os.environ.get("PG_PORT", 5432)),
            "dbname":   os.environ["PG_DBNAME"],
            "user":     os.environ["PG_USER"],
            "password": os.environ["PG_PASSWORD"],
        },
        "dest": {
            "host":     os.environ["SUPA_HOST"],
            "port":     int(os.environ.get("SUPA_PORT", 5432)),
            "dbname":   os.environ["SUPA_DBNAME"],
            "user":     os.environ["SUPA_USER"],
            "password": os.environ["SUPA_PASSWORD"],
        },
        "batch_size":      int(os.environ.get("BATCH_SIZE", 1000)),
        "exclude_schemas": excluded_schemas,
        "exclude_tables":  {t.strip() for t in raw_tables.split(",") if t.strip()},
        "log_level":       os.environ.get("LOG_LEVEL", "INFO"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 1 — CONNECTION TEST
# ─────────────────────────────────────────────────────────────────────────────
def connect_with_retry(
    params: dict, label: str, logger: logging.Logger, max_retries: int = 3
) -> psycopg2.extensions.connection:
    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**params, connect_timeout=10)
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"[{label}] Tentativa {attempt}/{max_retries} falhou: {e}")
            if attempt < max_retries:
                time.sleep(delay)
                delay *= 2
            else:
                raise


def test_connections(config: dict, logger: logging.Logger) -> tuple:
    console.print(Panel("[bold]STAGE 1 — Testando Conexões[/bold]", style="blue"))

    logger.info("Conectando ao PostgreSQL de origem...")
    src_conn = connect_with_retry(config["source"], "origem", logger)
    with src_conn.cursor() as cur:
        cur.execute("SELECT version();")
        src_version = cur.fetchone()[0].split(",")[0]
    logger.info(
        f"✅  Origem conectada  → [bold green]{src_version}[/bold green]"
        f"  (DB: {config['source']['dbname']})"
    )

    logger.info("Conectando ao Supabase de destino...")
    dst_conn = connect_with_retry(config["dest"], "destino", logger)
    with dst_conn.cursor() as cur:
        cur.execute("SELECT version();")
        dst_version = cur.fetchone()[0].split(",")[0]
    logger.info(
        f"✅  Destino conectado → [bold green]{dst_version}[/bold green]"
        f"  (DB: {config['dest']['dbname']})"
    )

    return src_conn, dst_conn


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 2 — INTROSPECTION
# ─────────────────────────────────────────────────────────────────────────────
def introspect(
    src_conn: psycopg2.extensions.connection, config: dict, logger: logging.Logger
) -> dict:
    console.print(Panel("[bold]STAGE 2 — Mapeando Banco de Origem[/bold]", style="blue"))

    exclude_schemas = list(config["exclude_schemas"])
    exclude_tables  = config["exclude_tables"]
    result = {}

    with src_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        # Extensions
        logger.info("Mapeando extensions...")
        cur.execute("""
            SELECT extname, extversion
            FROM pg_extension
            WHERE extname != 'plpgsql'
            ORDER BY extname;
        """)
        result["extensions"] = cur.fetchall()

        # Schemas
        logger.info("Mapeando schemas...")
        cur.execute("""
            SELECT nspname AS schema_name
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%%'
              AND nspname != 'information_schema'
              AND nspname != ANY(%s)
            ORDER BY nspname;
        """, (exclude_schemas,))
        result["schemas"] = [r["schema_name"] for r in cur.fetchall()] or ["public"]
        schemas_filter = result["schemas"]

        # Sequences
        logger.info("Mapeando sequences...")
        cur.execute("""
            SELECT sequence_schema, sequence_name, data_type,
                   start_value, minimum_value, maximum_value,
                   increment, cycle_option
            FROM information_schema.sequences
            WHERE sequence_schema = ANY(%s)
            ORDER BY sequence_schema, sequence_name;
        """, (schemas_filter,))
        result["sequences"] = cur.fetchall()

        # Tables
        logger.info("Mapeando tabelas...")
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ANY(%s)
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name;
        """, (schemas_filter,))
        tables_raw = [
            t for t in cur.fetchall()
            if t["table_name"] not in exclude_tables
            and f"{t['table_schema']}.{t['table_name']}" not in exclude_tables
        ]

        # Columns per table
        logger.info("Mapeando colunas...")
        tables = {}
        for t in tables_raw:
            fqn = f"{t['table_schema']}.{t['table_name']}"
            cur.execute("""
                SELECT column_name, ordinal_position, column_default,
                       is_nullable, data_type, character_maximum_length,
                       numeric_precision, numeric_scale,
                       udt_name, udt_schema,
                       is_identity, identity_generation,
                       is_generated, generation_expression
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """, (t["table_schema"], t["table_name"]))
            tables[fqn] = {
                "schema":  t["table_schema"],
                "name":    t["table_name"],
                "columns": cur.fetchall(),
            }
        result["tables"] = tables

        # Primary Keys
        logger.info("Mapeando chaves primárias...")
        cur.execute("""
            SELECT n.nspname AS schema_name, t.relname AS table_name,
                   c.conname AS constraint_name,
                   array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS columns
            FROM pg_constraint c
            JOIN pg_class t     ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'p' AND n.nspname = ANY(%s)
            GROUP BY n.nspname, t.relname, c.conname
            ORDER BY n.nspname, t.relname;
        """, (schemas_filter,))
        result["primary_keys"] = cur.fetchall()

        # Foreign Keys
        logger.info("Mapeando chaves estrangeiras...")
        cur.execute("""
            SELECT n.nspname  AS schema_name, t.relname  AS table_name,
                   c.conname  AS constraint_name,
                   array_agg(a.attname  ORDER BY array_position(c.conkey,  a.attnum))  AS columns,
                   rn.nspname AS ref_schema,   rt.relname AS ref_table,
                   array_agg(ra.attname ORDER BY array_position(c.confkey, ra.attnum)) AS ref_columns,
                   c.confupdtype AS on_update,  c.confdeltype AS on_delete
            FROM pg_constraint c
            JOIN pg_class t     ON t.oid  = c.conrelid
            JOIN pg_namespace n ON n.oid  = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid  AND a.attnum  = ANY(c.conkey)
            JOIN pg_class rt    ON rt.oid = c.confrelid
            JOIN pg_namespace rn ON rn.oid = rt.relnamespace
            JOIN pg_attribute ra ON ra.attrelid = rt.oid AND ra.attnum = ANY(c.confkey)
            WHERE c.contype = 'f' AND n.nspname = ANY(%s)
            GROUP BY n.nspname, t.relname, c.conname, rn.nspname, rt.relname, c.confupdtype, c.confdeltype
            ORDER BY n.nspname, t.relname;
        """, (schemas_filter,))
        result["foreign_keys"] = cur.fetchall()

        # Unique Constraints
        logger.info("Mapeando constraints unique...")
        cur.execute("""
            SELECT n.nspname AS schema_name, t.relname AS table_name,
                   c.conname AS constraint_name,
                   array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS columns
            FROM pg_constraint c
            JOIN pg_class t     ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE c.contype = 'u' AND n.nspname = ANY(%s)
            GROUP BY n.nspname, t.relname, c.conname
            ORDER BY n.nspname, t.relname;
        """, (schemas_filter,))
        result["unique_constraints"] = cur.fetchall()

        # Check Constraints
        logger.info("Mapeando check constraints...")
        cur.execute("""
            SELECT n.nspname AS schema_name, t.relname AS table_name,
                   c.conname AS constraint_name,
                   pg_get_constraintdef(c.oid) AS definition
            FROM pg_constraint c
            JOIN pg_class t     ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE c.contype = 'c'
              AND n.nspname = ANY(%s)
              AND c.conname NOT LIKE '%%_not_null'
            ORDER BY n.nspname, t.relname;
        """, (schemas_filter,))
        result["check_constraints"] = cur.fetchall()

        # Indexes (excluding PK/UNIQUE constraint indexes)
        logger.info("Mapeando indexes...")
        cur.execute("""
            SELECT schemaname AS schema_name, tablename AS table_name,
                   indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = ANY(%s)
              AND indexname NOT IN (
                  SELECT conname FROM pg_constraint WHERE contype IN ('p','u')
              )
            ORDER BY schemaname, tablename, indexname;
        """, (schemas_filter,))
        result["indexes"] = cur.fetchall()

        # Views
        logger.info("Mapeando views...")
        cur.execute("""
            SELECT table_schema AS schema_name, table_name AS view_name,
                   view_definition
            FROM information_schema.views
            WHERE table_schema = ANY(%s)
            ORDER BY table_schema, table_name;
        """, (schemas_filter,))
        result["views"] = cur.fetchall()

        # Materialized Views
        logger.info("Mapeando materialized views...")
        cur.execute("""
            SELECT schemaname AS schema_name, matviewname AS view_name, definition
            FROM pg_matviews
            WHERE schemaname = ANY(%s)
            ORDER BY schemaname, matviewname;
        """, (schemas_filter,))
        result["matviews"] = cur.fetchall()

        # Functions / Procedures
        logger.info("Mapeando functions e procedures...")
        cur.execute("""
            SELECT n.nspname AS schema_name, p.proname AS func_name,
                   pg_get_functiondef(p.oid) AS definition, p.prokind AS kind
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = ANY(%s)
              AND p.prokind IN ('f', 'p')
            ORDER BY n.nspname, p.proname;
        """, (schemas_filter,))
        result["functions"] = cur.fetchall()

        # Triggers
        logger.info("Mapeando triggers...")
        cur.execute("""
            SELECT DISTINCT
                   trg.trigger_schema AS schema_name,
                   trg.trigger_name,
                   trg.event_object_table AS table_name,
                   pg_get_triggerdef(pg_trg.oid) AS definition
            FROM information_schema.triggers trg
            JOIN pg_trigger pg_trg
              ON pg_trg.tgname = trg.trigger_name
             AND pg_trg.tgrelid = (
                 quote_ident(trg.trigger_schema) || '.' || quote_ident(trg.event_object_table)
             )::regclass
            WHERE trg.trigger_schema = ANY(%s)
              AND NOT pg_trg.tgisinternal
            ORDER BY trg.trigger_schema, trg.event_object_table, trg.trigger_name;
        """, (schemas_filter,))
        result["triggers"] = cur.fetchall()

    # Summary
    summary = Table(title="Mapeamento Concluído", header_style="bold magenta")
    summary.add_column("Objeto",     style="cyan")
    summary.add_column("Qtd", justify="right", style="bold green")
    rows = [
        ("Schemas",              len(result["schemas"])),
        ("Extensions",           len(result["extensions"])),
        ("Sequences",            len(result["sequences"])),
        ("Tabelas",              len(result["tables"])),
        ("Primary Keys",         len(result["primary_keys"])),
        ("Foreign Keys",         len(result["foreign_keys"])),
        ("Unique Constraints",   len(result["unique_constraints"])),
        ("Check Constraints",    len(result["check_constraints"])),
        ("Indexes",              len(result["indexes"])),
        ("Views",                len(result["views"])),
        ("Materialized Views",   len(result["matviews"])),
        ("Functions/Procedures", len(result["functions"])),
        ("Triggers",             len(result["triggers"])),
    ]
    for label, count in rows:
        summary.add_row(label, str(count))
    console.print(summary)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# DDL HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _col_type(col: dict) -> str:
    dt  = col["data_type"].upper()
    udt = (col.get("udt_name") or "").lower()

    if col.get("is_identity") == "YES":
        gen = col.get("identity_generation") or "BY DEFAULT"
        return f"GENERATED {gen} AS IDENTITY"

    if col.get("is_generated") == "ALWAYS" and col.get("generation_expression"):
        base = _col_type({**col, "is_generated": None, "is_identity": None})
        return f"{base} GENERATED ALWAYS AS ({col['generation_expression']}) STORED"

    if dt in ("CHARACTER VARYING", "CHARACTER"):
        ml = col.get("character_maximum_length")
        return f"VARCHAR({ml})" if ml else "TEXT"

    if dt == "NUMERIC":
        p, s = col.get("numeric_precision"), col.get("numeric_scale")
        return f"NUMERIC({p},{s})" if p is not None else "NUMERIC"

    if dt in ("USER-DEFINED", "ARRAY"):
        inner = udt[1:] if udt.startswith("_") else udt
        suffix = "[]" if dt == "ARRAY" or udt.startswith("_") else ""
        return f"{inner.upper()}{suffix}" if inner else "TEXT"

    SIMPLE = {
        "INTEGER":                     "INTEGER",
        "BIGINT":                      "BIGINT",
        "SMALLINT":                    "SMALLINT",
        "BOOLEAN":                     "BOOLEAN",
        "TEXT":                        "TEXT",
        "REAL":                        "REAL",
        "DOUBLE PRECISION":            "DOUBLE PRECISION",
        "BYTEA":                       "BYTEA",
        "UUID":                        "UUID",
        "DATE":                        "DATE",
        "TIME WITHOUT TIME ZONE":      "TIME",
        "TIME WITH TIME ZONE":         "TIMETZ",
        "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE":    "TIMESTAMPTZ",
        "INTERVAL":                    "INTERVAL",
        "JSON":                        "JSON",
        "JSONB":                       "JSONB",
        "INET":                        "INET",
        "CIDR":                        "CIDR",
        "MACADDR":                     "MACADDR",
        "BIT":                         "BIT",
        "BIT VARYING":                 "BIT VARYING",
        "MONEY":                       "MONEY",
        "OID":                         "OID",
        "TSVECTOR":                    "TSVECTOR",
        "TSQUERY":                     "TSQUERY",
        "XML":                         "XML",
        "NAME":                        "NAME",
    }
    return SIMPLE.get(dt, dt)


def _build_column_ddl(col: dict) -> str:
    name = f'"{col["column_name"]}"'

    if col.get("is_identity") == "YES":
        gen = col.get("identity_generation") or "BY DEFAULT"
        nullable = "NOT NULL" if col["is_nullable"] == "NO" else ""
        parts = [name, "INTEGER", f"GENERATED {gen} AS IDENTITY"]
        if nullable:
            parts.append(nullable)
        return " ".join(parts)

    if col.get("is_generated") == "ALWAYS" and col.get("generation_expression"):
        base_col = {**col, "is_generated": None, "is_identity": None}
        base_type = _col_type(base_col)
        return f'{name} {base_type} GENERATED ALWAYS AS ({col["generation_expression"]}) STORED'

    col_type = _col_type(col)
    nullable  = "NOT NULL" if col["is_nullable"] == "NO" else ""
    default   = col.get("column_default") or ""

    # Drop sequence-based defaults — sequences are created separately
    if default and "nextval(" in default:
        default = ""

    parts = [name, col_type]
    if default:
        parts.append(f"DEFAULT {default}")
    if nullable:
        parts.append(nullable)
    return " ".join(parts)


def _topological_sort(tables: dict, foreign_keys: list) -> list:
    in_degree  = {fqn: 0 for fqn in tables}
    dependents: dict = defaultdict(set)

    for fk in foreign_keys:
        src = f"{fk['schema_name']}.{fk['table_name']}"
        ref = f"{fk['ref_schema']}.{fk['ref_table']}"
        if src in tables and ref in tables and src != ref:
            if src not in dependents[ref]:
                dependents[ref].add(src)
                in_degree[src] += 1

    queue = deque(fqn for fqn, deg in in_degree.items() if deg == 0)
    order: list = []

    while queue:
        fqn = queue.popleft()
        order.append(fqn)
        for dep in list(dependents[fqn]):
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)

    # Append tables still in cycles (self-references, circular FKs)
    seen = set(order)
    order.extend(fqn for fqn in tables if fqn not in seen)
    return order


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 3 — APPLY SCHEMA DDL
# ─────────────────────────────────────────────────────────────────────────────
def apply_schema(
    dst_conn: psycopg2.extensions.connection, schema: dict, logger: logging.Logger
) -> list:
    console.print(Panel("[bold]STAGE 3 — Aplicando Schema no Supabase[/bold]", style="blue"))
    errors: list = []

    def exec_ddl(cur, sql: str, description: str):
        try:
            cur.execute(sql)
            dst_conn.commit()
            logger.info(f"✅  {description}")
        except Exception as exc:
            dst_conn.rollback()
            msg = str(exc).strip().replace("\n", " ")
            logger.warning(f"❌  {description}\n    [red]{msg}[/red]")
            errors.append({"object": description, "error": msg, "sql": sql[:300]})

    with dst_conn.cursor() as cur:

        # 1. Extensions
        console.print("\n[bold cyan]── Extensions[/bold cyan]")
        for ext in schema["extensions"]:
            exec_ddl(cur, f'CREATE EXTENSION IF NOT EXISTS "{ext["extname"]}";',
                     f"EXTENSION {ext['extname']}")

        # 2. Schemas
        console.print("\n[bold cyan]── Schemas[/bold cyan]")
        for s in schema["schemas"]:
            if s != "public":
                exec_ddl(cur, f'CREATE SCHEMA IF NOT EXISTS "{s}";', f"SCHEMA {s}")

        # 3. Sequences
        console.print("\n[bold cyan]── Sequences[/bold cyan]")
        for seq in schema["sequences"]:
            fqn   = f'"{seq["sequence_schema"]}"."{seq["sequence_name"]}"'
            cycle = "CYCLE" if seq.get("cycle_option") == "YES" else "NO CYCLE"
            sql = (
                f"CREATE SEQUENCE IF NOT EXISTS {fqn}\n"
                f"    INCREMENT {seq['increment']}\n"
                f"    MINVALUE  {seq['minimum_value']}\n"
                f"    MAXVALUE  {seq['maximum_value']}\n"
                f"    START     {seq['start_value']}\n"
                f"    {cycle};"
            )
            exec_ddl(cur, sql, f"SEQUENCE {fqn}")

        # 4. Tables (no FK constraints yet)
        console.print("\n[bold cyan]── Tabelas[/bold cyan]")
        table_order = _topological_sort(schema["tables"], schema["foreign_keys"])
        for fqn in table_order:
            tbl      = schema["tables"][fqn]
            cols_ddl = [_build_column_ddl(c) for c in tbl["columns"]]
            cols_str = ",\n    ".join(cols_ddl)
            sql = (
                f'CREATE TABLE IF NOT EXISTS "{tbl["schema"]}"."{tbl["name"]}" (\n'
                f"    {cols_str}\n);"
            )
            exec_ddl(cur, sql, f"TABLE {fqn}")

        # 5. Primary Keys
        console.print("\n[bold cyan]── Primary Keys[/bold cyan]")
        for pk in schema["primary_keys"]:
            fqn  = f'"{pk["schema_name"]}"."{pk["table_name"]}"'
            cols = ", ".join(f'"{c}"' for c in pk["columns"])
            exec_ddl(
                cur,
                f'ALTER TABLE {fqn} ADD CONSTRAINT "{pk["constraint_name"]}" PRIMARY KEY ({cols});',
                f"PK {pk['constraint_name']} → {fqn}",
            )

        # 6. Unique Constraints
        console.print("\n[bold cyan]── Unique Constraints[/bold cyan]")
        for uc in schema["unique_constraints"]:
            fqn  = f'"{uc["schema_name"]}"."{uc["table_name"]}"'
            cols = ", ".join(f'"{c}"' for c in uc["columns"])
            exec_ddl(
                cur,
                f'ALTER TABLE {fqn} ADD CONSTRAINT "{uc["constraint_name"]}" UNIQUE ({cols});',
                f"UNIQUE {uc['constraint_name']} → {fqn}",
            )

        # 7. Check Constraints
        console.print("\n[bold cyan]── Check Constraints[/bold cyan]")
        for cc in schema["check_constraints"]:
            fqn = f'"{cc["schema_name"]}"."{cc["table_name"]}"'
            exec_ddl(
                cur,
                f'ALTER TABLE {fqn} ADD CONSTRAINT "{cc["constraint_name"]}" {cc["definition"]};',
                f"CHECK {cc['constraint_name']} → {fqn}",
            )

        # 8. Foreign Keys (after all tables exist)
        console.print("\n[bold cyan]── Foreign Keys[/bold cyan]")
        for fk in schema["foreign_keys"]:
            fqn     = f'"{fk["schema_name"]}"."{fk["table_name"]}"'
            ref_fqn = f'"{fk["ref_schema"]}"."{fk["ref_table"]}"'
            cols     = ", ".join(f'"{c}"' for c in fk["columns"])
            ref_cols = ", ".join(f'"{c}"' for c in fk["ref_columns"])
            on_upd   = FK_ACTION_MAP.get(fk["on_update"], "NO ACTION")
            on_del   = FK_ACTION_MAP.get(fk["on_delete"], "NO ACTION")
            sql = (
                f'ALTER TABLE {fqn}\n'
                f'    ADD CONSTRAINT "{fk["constraint_name"]}"\n'
                f'    FOREIGN KEY ({cols}) REFERENCES {ref_fqn} ({ref_cols})\n'
                f'    ON UPDATE {on_upd} ON DELETE {on_del};'
            )
            exec_ddl(cur, sql, f"FK {fk['constraint_name']} → {fqn}")

        # 9. Indexes
        console.print("\n[bold cyan]── Indexes[/bold cyan]")
        for idx in schema["indexes"]:
            exec_ddl(
                cur,
                f"{idx['indexdef']};",
                f"INDEX {idx['indexname']} → {idx['schema_name']}.{idx['table_name']}",
            )

        # 10. Views
        console.print("\n[bold cyan]── Views[/bold cyan]")
        for v in schema["views"]:
            fqn = f'"{v["schema_name"]}"."{v["view_name"]}"'
            exec_ddl(cur, f"CREATE OR REPLACE VIEW {fqn} AS\n{v['view_definition']};",
                     f"VIEW {fqn}")

        # 11. Materialized Views
        console.print("\n[bold cyan]── Materialized Views[/bold cyan]")
        for mv in schema["matviews"]:
            fqn = f'"{mv["schema_name"]}"."{mv["view_name"]}"'
            exec_ddl(cur, f"CREATE MATERIALIZED VIEW IF NOT EXISTS {fqn} AS\n{mv['definition']};",
                     f"MATVIEW {fqn}")

        # 12. Functions / Procedures
        console.print("\n[bold cyan]── Functions / Procedures[/bold cyan]")
        for fn in schema["functions"]:
            kind = "FUNCTION" if fn["kind"] == "f" else "PROCEDURE"
            exec_ddl(cur, f"{fn['definition']};",
                     f"{kind} {fn['schema_name']}.{fn['func_name']}")

        # 13. Triggers
        console.print("\n[bold cyan]── Triggers[/bold cyan]")
        for tg in schema["triggers"]:
            if tg.get("definition"):
                exec_ddl(cur, f"{tg['definition']};",
                         f"TRIGGER {tg['trigger_name']} → {tg['schema_name']}.{tg['table_name']}")

    logger.info(
        f"Schema aplicado. [yellow]{len(errors)}[/yellow] erro(s) não-fatal(is) registrado(s)."
    )
    return errors


# ─────────────────────────────────────────────────────────────────────────────
# DATA SERIALIZATION HELPER
# ─────────────────────────────────────────────────────────────────────────────
def _serialize_row(row: tuple) -> tuple:
    result = []
    for val in row:
        if val is None:
            result.append(None)
        elif isinstance(val, dict):
            result.append(json.dumps(val, default=str))
        elif isinstance(val, memoryview):
            result.append(bytes(val))
        else:
            result.append(val)
    return tuple(result)


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 4 — DATA MIGRATION
# ─────────────────────────────────────────────────────────────────────────────
def _execute_batch_with_retry(
    dst_cur, dst_conn, insert_sql: str, batch: list,
    fqn: str, batch_size: int, errors: list, logger: logging.Logger,
) -> int:
    try:
        psycopg2.extras.execute_batch(dst_cur, insert_sql, batch, page_size=batch_size)
        dst_conn.commit()
        return len(batch)
    except Exception as exc:
        dst_conn.rollback()
        logger.warning(f"⚠  Batch falhou em {fqn}, tentando mini-batches de 100 linhas... ({exc})")

    inserted = 0
    for i in range(0, len(batch), 100):
        mini = batch[i : i + 100]
        try:
            psycopg2.extras.execute_batch(dst_cur, insert_sql, mini, page_size=100)
            dst_conn.commit()
            inserted += len(mini)
        except Exception as exc2:
            dst_conn.rollback()
            msg = str(exc2).strip().replace("\n", " ")
            logger.error(f"❌  Mini-batch falhou em {fqn}: [red]{msg}[/red]")
            errors.append({"object": f"DATA BATCH {fqn}", "error": msg, "sql": insert_sql[:200]})
    return inserted


def migrate_data(
    src_conn: psycopg2.extensions.connection,
    dst_conn: psycopg2.extensions.connection,
    schema: dict,
    config: dict,
    logger: logging.Logger,
) -> list:
    console.print(Panel("[bold]STAGE 4 — Migrando Dados[/bold]", style="blue"))

    batch_size  = config["batch_size"]
    table_order = _topological_sort(schema["tables"], schema["foreign_keys"])
    errors: list = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        outer = progress.add_task("[bold cyan]Tabelas[/bold cyan]", total=len(table_order))

        for fqn in table_order:
            tbl = schema["tables"][fqn]
            quoted_fqn = f'"{tbl["schema"]}"."{tbl["name"]}"'

            # Exclude generated columns from insert
            insertable_cols = [
                c for c in tbl["columns"]
                if c.get("is_generated") != "ALWAYS"
            ]
            if not insertable_cols:
                progress.advance(outer)
                continue

            col_names = [f'"{c["column_name"]}"' for c in insertable_cols]

            # Row count
            try:
                with src_conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {quoted_fqn};")
                    total_rows: int = cur.fetchone()[0]
            except Exception as exc:
                logger.warning(f"⚠  Não foi possível contar linhas de {fqn}: {exc}")
                errors.append({"object": f"COUNT {fqn}", "error": str(exc), "sql": ""})
                progress.advance(outer)
                continue

            progress.update(outer, description=f"[cyan]{fqn}[/cyan] ({total_rows:,} linhas)")
            inner = progress.add_task(f"  ↳ {tbl['name']}", total=max(total_rows, 1))

            with dst_conn.cursor() as dst_cur:
                try:
                    dst_cur.execute(f"ALTER TABLE {quoted_fqn} DISABLE TRIGGER ALL;")
                    dst_conn.commit()

                    dst_cur.execute(f"TRUNCATE TABLE {quoted_fqn} CASCADE;")
                    dst_conn.commit()

                    placeholders = ", ".join(["%s"] * len(col_names))
                    insert_sql = (
                        f"INSERT INTO {quoted_fqn} ({', '.join(col_names)}) "
                        f"VALUES ({placeholders});"
                    )

                    server_cursor_name = f"mig_{tbl['name'][:24]}_{int(time.time())}"
                    with src_conn.cursor(
                        name=server_cursor_name,
                        cursor_factory=psycopg2.extras.RealDictCursor,
                    ) as src_cur:
                        src_cur.itersize = batch_size
                        src_cur.execute(
                            f"SELECT {', '.join(col_names)} FROM {quoted_fqn};"
                        )

                        batch: list = []
                        inserted = 0

                        for row in src_cur:
                            batch.append(_serialize_row(tuple(row.values())))
                            if len(batch) >= batch_size:
                                n = _execute_batch_with_retry(
                                    dst_cur, dst_conn, insert_sql, batch,
                                    fqn, batch_size, errors, logger,
                                )
                                inserted += n
                                progress.update(inner, advance=len(batch))
                                batch = []

                        if batch:
                            n = _execute_batch_with_retry(
                                dst_cur, dst_conn, insert_sql, batch,
                                fqn, batch_size, errors, logger,
                            )
                            inserted += n
                            progress.update(inner, advance=len(batch))

                    dst_cur.execute(f"ALTER TABLE {quoted_fqn} ENABLE TRIGGER ALL;")
                    dst_conn.commit()
                    logger.info(f"✅  {fqn}  —  {inserted:,} linhas inseridas")

                except Exception as exc:
                    msg = str(exc).strip().replace("\n", " ")
                    logger.error(f"❌  Erro ao migrar {fqn}: [red]{msg}[/red]")
                    errors.append({"object": f"DATA {fqn}", "error": msg, "sql": ""})
                    try:
                        dst_conn.rollback()
                        dst_cur.execute(f"ALTER TABLE {quoted_fqn} ENABLE TRIGGER ALL;")
                        dst_conn.commit()
                    except Exception:
                        pass

            progress.update(inner, completed=max(total_rows, 1))
            progress.advance(outer)

    return errors


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 5 — VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
def validate(
    src_conn: psycopg2.extensions.connection,
    dst_conn: psycopg2.extensions.connection,
    schema: dict,
    logger: logging.Logger,
) -> tuple:
    console.print(Panel("[bold]STAGE 5 — Validando Contagem de Linhas[/bold]", style="blue"))

    results: list = []
    all_ok = True

    tbl = Table(title="Validação por Tabela", header_style="bold magenta")
    tbl.add_column("Tabela",  style="cyan", no_wrap=True)
    tbl.add_column("Origem",  justify="right")
    tbl.add_column("Destino", justify="right")
    tbl.add_column("Status",  justify="center")

    for fqn, info in schema["tables"].items():
        quoted_fqn = f'"{info["schema"]}"."{info["name"]}"'
        try:
            with src_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {quoted_fqn};")
                src_count = cur.fetchone()[0]
            with dst_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {quoted_fqn};")
                dst_count = cur.fetchone()[0]

            ok     = src_count == dst_count
            status = "✅" if ok else "❌"
            if not ok:
                all_ok = False
            tbl.add_row(fqn, f"{src_count:,}", f"{dst_count:,}", status)
            results.append({"table": fqn, "src": src_count, "dst": dst_count, "ok": ok})
        except Exception as exc:
            all_ok = False
            tbl.add_row(fqn, "ERR", "ERR", "⚠")
            results.append({"table": fqn, "src": "?", "dst": "?", "ok": False})
            logger.warning(f"⚠  Erro na validação de {fqn}: {exc}")

    console.print(tbl)
    return results, all_ok


# ─────────────────────────────────────────────────────────────────────────────
# STAGE 6 — FINAL REPORT
# ─────────────────────────────────────────────────────────────────────────────
def final_report(
    schema: dict,
    schema_errors: list,
    data_errors: list,
    validation_results: list,
    all_ok: bool,
    start_time: float,
    logger: logging.Logger,
):
    console.print(Panel("[bold]STAGE 6 — Relatório Final[/bold]", style="blue"))

    elapsed         = time.time() - start_time
    minutes, secs   = divmod(int(elapsed), 60)
    all_errors      = schema_errors + data_errors
    failed_val      = [v for v in validation_results if not v["ok"]]

    if not all_errors and all_ok:
        status_color, status_text = "green",  "SUCESSO COMPLETO ✅"
    elif not all_errors:
        status_color, status_text = "yellow", "CONCLUÍDO COM AVISOS ⚠"
    else:
        status_color, status_text = "red",    "CONCLUÍDO COM ERROS ❌"

    summary_lines = [
        f"[bold]Duração total:[/bold]           {minutes}m {secs}s",
        f"[bold]Tabelas migradas:[/bold]         {len(schema['tables'])}",
        f"[bold]Views:[/bold]                    {len(schema['views'])}",
        f"[bold]Materialized Views:[/bold]       {len(schema['matviews'])}",
        f"[bold]Functions / Procedures:[/bold]   {len(schema['functions'])}",
        f"[bold]Triggers:[/bold]                 {len(schema['triggers'])}",
        f"[bold]Erros de schema:[/bold]          {len(schema_errors)}",
        f"[bold]Erros de dados:[/bold]           {len(data_errors)}",
        f"[bold]Tabelas com divergência:[/bold]  {len(failed_val)}",
        f"[bold]Log salvo em:[/bold]             {LOG_FILE}",
    ]
    console.print(Panel(
        "\n".join(summary_lines),
        title=f"[bold {status_color}]{status_text}[/bold {status_color}]",
        border_style=status_color,
        padding=(1, 2),
    ))

    if all_errors:
        console.print("\n[bold red]── Erros Encontrados ──[/bold red]")
        err_tbl = Table(header_style="bold red", show_lines=True)
        err_tbl.add_column("Objeto", style="yellow", max_width=45, no_wrap=True)
        err_tbl.add_column("Erro",   style="red",    max_width=80)
        for err in all_errors:
            err_tbl.add_row(err["object"], err["error"])
        console.print(err_tbl)

    logger.info(
        f"Migração concluída em {minutes}m {secs}s — "
        f"Erros: {len(all_errors)} — Divergências: {len(failed_val)}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    console.print(Panel(
        "[bold white]PostgreSQL → Supabase Migration Tool[/bold white]\n"
        "[dim]Migração completa de schema + dados via conexão direta PostgreSQL[/dim]",
        style="bold blue",
        padding=(1, 4),
    ))

    start_time = time.time()

    # ── Stage 0 — Credentials ────────────────────────────────
    console.print(Panel("[bold]STAGE 0 — Configuração de Credenciais[/bold]", style="blue"))
    config = load_credentials()
    logger = setup_logging(config["log_level"])
    logger.info(f"Log iniciado → {LOG_FILE}")
    logger.info(
        f"Batch size: {config['batch_size']} | "
        f"Schemas excluídos: {', '.join(sorted(config['exclude_schemas']))}"
    )

    # ── Stage 1 — Connections ─────────────────────────────────
    try:
        src_conn, dst_conn = test_connections(config, logger)
    except Exception as exc:
        console.print(f"\n[bold red]Falha crítica ao conectar:[/bold red] {exc}\n")
        sys.exit(1)

    try:
        # ── Stage 2 — Introspection ───────────────────────────
        schema = introspect(src_conn, config, logger)
        if not schema["tables"]:
            logger.warning(
                "Nenhuma tabela encontrada no banco de origem. "
                "Verifique os schemas e as permissões do usuário."
            )

        # ── Stage 3 — Schema DDL ──────────────────────────────
        schema_errors = apply_schema(dst_conn, schema, logger)

        # ── Stage 4 — Data ────────────────────────────────────
        data_errors = migrate_data(src_conn, dst_conn, schema, config, logger)

        # ── Stage 5 — Validation ──────────────────────────────
        validation_results, all_ok = validate(src_conn, dst_conn, schema, logger)

        # ── Stage 6 — Report ──────────────────────────────────
        final_report(
            schema, schema_errors, data_errors,
            validation_results, all_ok,
            start_time, logger,
        )

    except KeyboardInterrupt:
        console.print("\n[yellow]⚠  Migração interrompida pelo usuário (CTRL+C).[/yellow]")
        logger.warning("Migração interrompida pelo usuário.")
    except Exception as exc:
        logger.error(f"[bold red]Erro inesperado:[/bold red] {exc}", exc_info=True)
        sys.exit(1)
    finally:
        for conn in (src_conn, dst_conn):
            try:
                conn.close()
            except Exception:
                pass

    console.print(f"\n[dim]Log completo disponível em:[/dim] [bold]{LOG_FILE}[/bold]\n")


if __name__ == "__main__":
    main()
