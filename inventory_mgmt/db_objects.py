# db_objects.py
import os
import pyodbc
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

INV_DB_SERVER = os.getenv("INV_DB_SERVER")
INV_DB_USER = os.getenv("INV_DB_USER")
INV_DB_PASSWORD = os.getenv("INV_DB_PASSWORD")
INV_DB_TRUSTED = (os.getenv("INV_DB_TRUSTED", "YES").upper() == "YES")



def get_target_db_connection(database_name: str):
    """
    Connect to the TARGET SQL database (the one whose objects we inspect),
    not the central inventory DB.
    """
    if not INV_DB_SERVER:
        raise RuntimeError("INV_DB_SERVER is not set in .env")

    if INV_DB_TRUSTED:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={INV_DB_SERVER};"
            f"DATABASE={database_name};"
            "Trusted_Connection=yes;"
        )
    else:
        if not INV_DB_USER or not INV_DB_PASSWORD:
            raise RuntimeError("Using SQL auth but INV_DB_USER / INV_DB_PASSWORD not set")
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={INV_DB_SERVER};"
            f"DATABASE={database_name};"
            f"UID={INV_DB_USER};"
            f"PWD={INV_DB_PASSWORD};"
        )
    return pyodbc.connect(conn_str)


def get_db_user_count(database_name: str) -> int:
    """
    Count DB-level principals (users/roles) for overview.
    """
    conn = get_target_db_connection(database_name)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS user_count
            FROM sys.database_principals
            WHERE type IN ('S','U','G')   -- SQL, Windows, Windows group
              AND name NOT LIKE '##%';    -- ignore internal
            """
        )
        row = cur.fetchone()
        return int(row.user_count) if row and row.user_count is not None else 0
    finally:
        conn.close()


def get_db_object_lists(database_name: str):
    """
    Return lists of user objects by type for the given DB:
      {
        'TABLE':      [ {'schema':'dbo','name':'T1','full_name':'dbo.T1'}, ... ],
        'VIEW':       [ ... ],
        'PROCEDURE':  [ ... ],
        'FUNCTION':   [ ... ],
      }
    """
    conn = get_target_db_connection(database_name)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                'TABLE' AS object_type,
                s.name  AS schema_name,
                t.name  AS object_name
            FROM sys.tables AS t
            JOIN sys.schemas AS s ON t.schema_id = s.schema_id
            WHERE t.is_ms_shipped = 0

            UNION ALL

            SELECT
                'VIEW' AS object_type,
                s.name,
                v.name
            FROM sys.views AS v
            JOIN sys.schemas AS s ON v.schema_id = s.schema_id
            WHERE v.is_ms_shipped = 0

            UNION ALL

            SELECT
                'PROCEDURE' AS object_type,
                s.name,
                p.name
            FROM sys.procedures AS p
            JOIN sys.schemas AS s ON p.schema_id = s.schema_id
            WHERE p.is_ms_shipped = 0

            UNION ALL

            SELECT
                'FUNCTION' AS object_type,
                s.name,
                o.name
            FROM sys.objects AS o
            JOIN sys.schemas AS s ON o.schema_id = s.schema_id
            WHERE o.is_ms_shipped = 0
              AND o.type IN ('FN','IF','TF');  -- scalar, inline, table-valued
            """
        )

        lists = {
            "TABLE": [],
            "VIEW": [],
            "PROCEDURE": [],
            "FUNCTION": [],
        }

        for row in cur.fetchall():
            obj_type = row.object_type
            schema_name = row.schema_name
            object_name = row.object_name
            full_name = f"{schema_name}.{object_name}"
            lists[obj_type].append({
                "schema": schema_name,
                "name": object_name,
                "full_name": full_name,
            })

        # sort lists by schema.name
        for k in lists:
            lists[k].sort(key=lambda x: (x["schema"].lower(), x["name"].lower()))

        return lists
    finally:
        conn.close()


def get_object_metadata(database_name: str, object_type: str, full_name: str):
    """
    For a given object (table/view/procedure/function), return:
      - basic metadata (schema, name, type_desc, dates)
      - type-specific extras (rows/size/index/PK/FK/fragmentation for tables)
      - dependencies:
          referenced: objects this object depends on
          referencing: objects that depend on this object
      - referenced_summary: count of referenced tables/views/procs/functions
    object_type: 'TABLE' / 'VIEW' / 'PROCEDURE' / 'FUNCTION'
    full_name: 'schema.name'
    """
    conn = get_target_db_connection(database_name)
    cur = conn.cursor()
    try:
        # Split schema.name
        if "." in full_name:
            schema_name, object_name = full_name.split(".", 1)
        else:
            schema_name, object_name = "dbo", full_name

        # ---------- base object row ----------
        cur.execute(
            """
            SELECT
                o.object_id,
                s.name AS schema_name,
                o.name AS object_name,
                o.type_desc,
                o.create_date,
                o.modify_date
            FROM sys.objects AS o
            JOIN sys.schemas AS s ON o.schema_id = s.schema_id
            WHERE s.name = ?
              AND o.name = ?;
            """,
            (schema_name, object_name),
        )
        base = cur.fetchone()
        if not base:
            return None

        object_id = base.object_id

        meta = {
            "schema_name": base.schema_name,
            "object_name": base.object_name,
            "full_name": f"{base.schema_name}.{base.object_name}",
            "type_desc": base.type_desc,
            "create_date": base.create_date,
            "modify_date": base.modify_date,
            "extras": {},
            "referenced": [],
            "referencing": [],
            "referenced_summary": {},
        }

        obj_type_upper = object_type.upper()

        # ============================================================
        # TABLE-SPECIFIC METADATA
        # ============================================================
        if obj_type_upper == "TABLE":
            # --- row count & size via dm_db_partition_stats ---
            cur.execute(
                """
                SELECT
                    SUM(ps.[rows]) AS row_count,
                    CONVERT(DECIMAL(18,2),
                        SUM(ps.reserved_page_count) * 8.0 / 1024
                    ) AS reserved_mb,
                    CONVERT(DECIMAL(18,2),
                        SUM(ps.used_page_count) * 8.0 / 1024
                    ) AS used_mb
                FROM sys.dm_db_partition_stats AS ps
                WHERE ps.object_id = ?
                  AND ps.index_id IN (0,1);  -- heap or clustered
                """,
                (object_id,),
            )
            sz = cur.fetchone()
            row_count = int(sz.row_count) if sz and sz.row_count is not None else None
            reserved_mb = float(sz.reserved_mb) if sz and sz.reserved_mb is not None else None
            used_mb = float(sz.used_mb) if sz and sz.used_mb is not None else None

            # --- index structure (PK + columns + included cols) ---
            cur.execute(
                """
                SELECT
                    i.name AS index_name,
                    i.is_primary_key,
                    i.is_unique,
                    i.type_desc,
                    ic.key_ordinal,
                    ic.is_included_column,
                    c.name AS column_name
                FROM sys.indexes i
                JOIN sys.index_columns ic
                    ON i.object_id = ic.object_id
                   AND i.index_id = ic.index_id
                JOIN sys.columns c
                    ON c.object_id = ic.object_id
                   AND c.column_id = ic.column_id
                WHERE i.object_id = ?
                  AND i.index_id > 0
                  AND i.is_hypothetical = 0
                ORDER BY i.index_id, ic.key_ordinal, c.column_id;
                """,
                (object_id,),
            )

            index_map = {}  # index_name -> dict
            for row in cur.fetchall():
                idx_name = row.index_name
                if idx_name not in index_map:
                    index_map[idx_name] = {
                        "name": idx_name,
                        "is_primary_key": bool(row.is_primary_key),
                        "is_unique": bool(row.is_unique),
                        "type_desc": row.type_desc,
                        "key_columns": [],
                        "included_columns": [],
                        "fragmentation_percent": None,  # will fill later
                    }

                if row.is_included_column:
                    index_map[idx_name]["included_columns"].append(row.column_name)
                else:
                    # key_ordinal > 0 means a key column
                    index_map[idx_name]["key_columns"].append(row.column_name)

            # --- fragmentation via dm_db_index_physical_stats ---
            try:
                cur.execute(
                    """
                    SELECT
                        i.name AS index_name,
                        ips.avg_fragmentation_in_percent
                    FROM sys.indexes i
                    JOIN sys.dm_db_index_physical_stats(
                            DB_ID(), ?, NULL, NULL, 'SAMPLED'
                         ) AS ips
                      ON ips.object_id = i.object_id
                     AND ips.index_id = i.index_id
                    WHERE i.object_id = ?
                      AND i.index_id > 0
                      AND i.is_hypothetical = 0;
                    """,
                    (object_id, object_id),
                )
                for row in cur.fetchall():
                    idx_name = row.index_name
                    if idx_name in index_map:
                        index_map[idx_name]["fragmentation_percent"] = float(
                            row.avg_fragmentation_in_percent
                        ) if row.avg_fragmentation_in_percent is not None else None
            except Exception as ex:
                # If DMV not available, leave fragmentation as None
                print(f"[WARN] index fragmentation lookup failed: {ex}")

            # --- primary key info (from index_map) ---
            primary_key_info = None
            for idx in index_map.values():
                if idx["is_primary_key"]:
                    primary_key_info = {
                        "name": idx["name"],
                        "columns": idx["key_columns"],
                    }
                    break

            # --- foreign keys ---
            cur.execute(
                """
                SELECT
                    fk.name AS fk_name,
                    sch_parent.name AS parent_schema,
                    t_parent.name   AS parent_table,
                    sch_ref.name    AS ref_schema,
                    t_ref.name      AS ref_table,
                    pc.name         AS parent_column,
                    rc.name         AS ref_column,
                    fkc.constraint_column_id
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc
                    ON fk.object_id = fkc.constraint_object_id
                JOIN sys.tables t_parent
                    ON fk.parent_object_id = t_parent.object_id
                JOIN sys.schemas sch_parent
                    ON t_parent.schema_id = sch_parent.schema_id
                JOIN sys.tables t_ref
                    ON fk.referenced_object_id = t_ref.object_id
                JOIN sys.schemas sch_ref
                    ON t_ref.schema_id = sch_ref.schema_id
                JOIN sys.columns pc
                    ON pc.object_id = t_parent.object_id
                   AND pc.column_id = fkc.parent_column_id
                JOIN sys.columns rc
                    ON rc.object_id = t_ref.object_id
                   AND rc.column_id = fkc.referenced_column_id
                WHERE fk.parent_object_id = ?
                ORDER BY fk.name, fkc.constraint_column_id;
                """,
                (object_id,),
            )

            fk_map = {}  # fk_name -> dict
            for row in cur.fetchall():
                fk_name = row.fk_name
                if fk_name not in fk_map:
                    fk_map[fk_name] = {
                        "name": fk_name,
                        "parent_table": f"{row.parent_schema}.{row.parent_table}",
                        "ref_table": f"{row.ref_schema}.{row.ref_table}",
                        "parent_columns": [],
                        "ref_columns": [],
                    }
                fk_map[fk_name]["parent_columns"].append(row.parent_column)
                fk_map[fk_name]["ref_columns"].append(row.ref_column)

            foreign_keys = list(fk_map.values())
            index_list = list(index_map.values())

            meta["extras"] = {
                "row_count": row_count,
                "reserved_mb": reserved_mb,
                "used_mb": used_mb,
                "index_count": len(index_list),
                "primary_key": primary_key_info,
                "foreign_keys": foreign_keys,
                "indexes": index_list,
            }

        # ============================================================
        # NON-TABLE OBJECTS (VIEW / PROCEDURE / FUNCTION)
        # ============================================================
        elif obj_type_upper in ("VIEW", "PROCEDURE", "FUNCTION"):
            # You can extend this later with usage, stats, etc.
            meta["extras"] = {}

        # ============================================================
        # DEPENDENCIES
        # ============================================================

        # --- objects referenced by this object ---
        try:
            cur.execute(
                """
                SELECT DISTINCT
                    OBJECT_SCHEMA_NAME(d.referenced_id) AS ref_schema,
                    OBJECT_NAME(d.referenced_id)        AS ref_name,
                    o_ref.type_desc                     AS ref_type
                FROM sys.sql_expression_dependencies AS d
                LEFT JOIN sys.objects AS o_ref
                  ON o_ref.object_id = d.referenced_id
                WHERE d.referencing_id = ?
                  AND d.referenced_id IS NOT NULL;
                """,
                (object_id,),
            )
            for row in cur.fetchall():
                if row.ref_name is None:
                    continue
                meta["referenced"].append({
                    "schema": row.ref_schema,
                    "name": row.ref_name,
                    "type_desc": row.ref_type,
                    "full_name": f"{row.ref_schema}.{row.ref_name}"
                    if row.ref_schema and row.ref_name else row.ref_name,
                })
        except Exception as ex:
            print(f"[WARN] referenced deps failed: {ex}")

        # --- objects that reference this object ---
        try:
            cur.execute(
                """
                SELECT DISTINCT
                    OBJECT_SCHEMA_NAME(d.referencing_id) AS ref_schema,
                    OBJECT_NAME(d.referencing_id)        AS ref_name,
                    o_ref.type_desc                     AS ref_type
                FROM sys.sql_expression_dependencies AS d
                LEFT JOIN sys.objects AS o_ref
                  ON o_ref.object_id = d.referencing_id
                WHERE d.referenced_id = ?;
                """,
                (object_id,),
            )
            for row in cur.fetchall():
                if row.ref_name is None:
                    continue
                meta["referencing"].append({
                    "schema": row.ref_schema,
                    "name": row.ref_name,
                    "type_desc": row.ref_type,
                    "full_name": f"{row.ref_schema}.{row.ref_name}"
                    if row.ref_schema and row.ref_name else row.ref_name,
                })
        except Exception as ex:
            print(f"[WARN] referencing deps failed: {ex}")

        # --- referenced summary by type (tables/views/procs/functions) ---
        summary = {
            "TABLE": 0,
            "VIEW": 0,
            "PROCEDURE": 0,
            "FUNCTION": 0,
            "OTHER": 0,
        }
        for r in meta["referenced"]:
            t = (r["type_desc"] or "").upper()
            if "TABLE" in t:
                summary["TABLE"] += 1
            elif "VIEW" in t:
                summary["VIEW"] += 1
            elif "PROCEDURE" in t:
                summary["PROCEDURE"] += 1
            elif "FUNCTION" in t:
                summary["FUNCTION"] += 1
            else:
                summary["OTHER"] += 1
        meta["referenced_summary"] = summary

        return meta

    finally:
        conn.close()
