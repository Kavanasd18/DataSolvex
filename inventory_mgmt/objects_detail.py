# objects_detail.py
#
# Responsible for fetching detailed metadata for a single database object:
# tables, views, procedures, functions.
#
# Expected usage (typical):
#   meta = get_object_metadata(conn, db_name, "dbo.YourTable", "TABLE")
#
# Returned structure (dict) matches what objects.html expects:
#   {
#     "schema_name": ...,
#     "object_name": ...,
#     "full_name": "schema.object",
#     "type": "TABLE" | "VIEW" | "PROCEDURE" | "FUNCTION",
#     "type_desc": "...",
#     "create_date": datetime,
#     "modify_date": datetime,
#     "extras": { ... type-specific ... },
#     "referenced": [ { full_name, object_type, type_desc }, ... ],
#     "referencing": [ { full_name, object_type, type_desc }, ... ],
#   }

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pyodbc
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional




# ------------- helpers: type mapping / parsing -----------------


def _normalize_object_type(type_value: str) -> str:
    """Map UI string (table/view/procedure/function) to canonical key."""
    if not type_value:
        return "TABLE"
    v = type_value.strip().lower()
    if v in ("table", "tables"):
        return "TABLE"
    if v in ("view", "views"):
        return "VIEW"
    if v in ("procedure", "procedures", "proc", "sp"):
        return "PROCEDURE"
    if v in ("function", "functions", "func", "fn"):
        return "FUNCTION"
    return "TABLE"


def _split_full_name(full_name: str) -> Tuple[str, str]:
    """
    Split 'schema.object' into (schema, object).
    If schema missing, default to 'dbo'. Strip [] if user passed [dbo].[Table].
    """
    if not full_name:
        return "dbo", full_name

    name = full_name.strip()

    # Remove [] wrappers lightly
    if name.startswith("[") and name.endswith("]") and "].[" not in name:
        name = name[1:-1]

    if "." in name:
        schema, obj = name.split(".", 1)
    else:
        schema, obj = "dbo", name

    def clean(part: str) -> str:
        p = part.strip()
        if p.startswith("[") and p.endswith("]"):
            p = p[1:-1]
        return p

    return clean(schema), clean(obj)


def _map_sql_type_to_logical(o_type: str) -> Optional[str]:
    """Map sys.objects.type code to our logical types."""
    if o_type == "U":
        return "TABLE"
    if o_type == "V":
        return "VIEW"
    if o_type in ("P", "PC"):
        return "PROCEDURE"
    if o_type in ("FN", "IF", "TF", "FS", "FT"):
        return "FUNCTION"
    return None


# ------------- core metadata queries -----------------


def _get_basic_metadata(cursor, schema_name: str, object_name: str, obj_kind: str) -> Optional[Dict[str, Any]]:
    """
    Get base row from sys.objects + sys.schemas for the given name and logical type.
    """
    type_codes: List[str]
    if obj_kind == "TABLE":
        type_codes = ["U"]
    elif obj_kind == "VIEW":
        type_codes = ["V"]
    elif obj_kind == "PROCEDURE":
        type_codes = ["P", "PC"]
    elif obj_kind == "FUNCTION":
        type_codes = ["FN", "IF", "TF", "FS", "FT"]
    else:
        type_codes = ["U"]

    sql = """
    SELECT TOP (1)
        s.name AS schema_name,
        o.name AS object_name,
        o.object_id,
        o.type,
        o.type_desc,
        o.create_date,
        o.modify_date
    FROM sys.objects AS o
    JOIN sys.schemas AS s
        ON s.schema_id = o.schema_id
    WHERE s.name = ?
      AND o.name = ?
      AND o.type IN ({})
    ORDER BY o.object_id;
    """.format(",".join("?" for _ in type_codes))

    params = [schema_name, object_name] + type_codes
    cursor.execute(sql, params)
    row = cursor.fetchone()
    if not row:
        return None

    return {
        "schema_name": row.schema_name,
        "object_name": row.object_name,
        "object_id": row.object_id,
        "type_code": row.type,
        "type_desc": row.type_desc,
        "create_date": row.create_date,
        "modify_date": row.modify_date,
    }


# ------------- TABLE helpers -----------------


def _get_table_space_basic(cursor, full_name: str) -> Dict[str, Any]:
    """
    Row count + reserved/used MB for the whole table (all indexes).
    Uses sys.dm_db_partition_stats.
    """
    sql = """
    DECLARE @obj_id int = OBJECT_ID(?);

    SELECT
        SUM(CASE WHEN index_id IN (0,1) THEN row_count ELSE 0 END) AS row_count,
        SUM(reserved_page_count) * 8.0 / 1024.0 AS reserved_mb,
        SUM(used_page_count)     * 8.0 / 1024.0 AS used_mb
    FROM sys.dm_db_partition_stats
    WHERE object_id = @obj_id;
    """
    cursor.execute(sql, (full_name,))
    row = cursor.fetchone()
    if not row:
        return {"row_count": 0, "reserved_mb": 0.0, "used_mb": 0.0}
    return {
        "row_count": row.row_count or 0,
        "reserved_mb": float(row.reserved_mb or 0.0),
        "used_mb": float(row.used_mb or 0.0),
    }


def _get_table_indexes(cursor, full_name: str) -> List[Dict[str, Any]]:
    """
    Per-index details: name, type, PK, unique, key/included columns,
    fragmentation %, used/reserved MB.
    """
    sql = """
    DECLARE @obj_id int = OBJECT_ID(?);

    WITH idx_space AS (
        SELECT
            index_id,
            SUM(used_page_count)     * 8.0 / 1024.0 AS used_mb,
            SUM(reserved_page_count) * 8.0 / 1024.0 AS reserved_mb
        FROM sys.dm_db_partition_stats
        WHERE object_id = @obj_id
        GROUP BY index_id
    ),
    idx_cols AS (
        SELECT
            ic.object_id,
            ic.index_id,
            key_cols = STUFF((
                SELECT ', ' + QUOTENAME(COL_NAME(ic2.object_id, ic2.column_id))
                FROM sys.index_columns ic2
                WHERE ic2.object_id = ic.object_id
                  AND ic2.index_id  = ic.index_id
                  AND ic2.is_included_column = 0
                ORDER BY ic2.key_ordinal
                FOR XML PATH(''), TYPE
            ).value('.', 'nvarchar(max)'), 1, 2, ''),
            inc_cols = STUFF((
                SELECT ', ' + QUOTENAME(COL_NAME(ic3.object_id, ic3.column_id))
                FROM sys.index_columns ic3
                WHERE ic3.object_id = ic.object_id
                  AND ic3.index_id  = ic.index_id
                  AND ic3.is_included_column = 1
                ORDER BY ic3.index_column_id
                FOR XML PATH(''), TYPE
            ).value('.', 'nvarchar(max)'), 1, 2, '')
        FROM sys.index_columns ic
        GROUP BY ic.object_id, ic.index_id
    )
    SELECT
        i.name,
        i.index_id,
        i.type_desc,
        i.is_primary_key,
        i.is_unique,
        ISNULL(c.key_cols, '') AS key_columns,
        ISNULL(c.inc_cols, '') AS included_columns,
        ips.avg_fragmentation_in_percent,
        ISNULL(s.used_mb, 0.0)     AS used_mb,
        ISNULL(s.reserved_mb, 0.0) AS reserved_mb
    FROM sys.indexes i
    LEFT JOIN idx_space s
        ON s.index_id = i.index_id
       AND OBJECT_ID = @obj_id
    LEFT JOIN idx_cols c
        ON c.object_id = i.object_id
       AND c.index_id  = i.index_id
    LEFT JOIN sys.dm_db_index_physical_stats(DB_ID(), @obj_id, NULL, NULL, 'LIMITED') ips
        ON ips.object_id = i.object_id
       AND ips.index_id  = i.index_id
    WHERE i.object_id = @obj_id
      AND i.index_id > 0
      AND i.is_hypothetical = 0
    ORDER BY i.index_id;
    """
    cursor.execute(sql, (full_name,))
    rows = cursor.fetchall() or []
    indexes: List[Dict[str, Any]] = []
    for r in rows:
        indexes.append(
            {
                "name": r.name,
                "type_desc": r.type_desc,
                "is_primary_key": bool(r.is_primary_key),
                "is_unique": bool(r.is_unique),
                "key_columns": (r.key_columns or "").split(", ") if r.key_columns else [],
                "included_columns": (r.included_columns or "").split(", ")
                if r.included_columns
                else [],
                "fragmentation_percent": float(r.avg_fragmentation_in_percent)
                if r.avg_fragmentation_in_percent is not None
                else None,
                "used_mb": float(r.used_mb or 0.0),
                "reserved_mb": float(r.reserved_mb or 0.0),
            }
        )
    return indexes


def _get_table_partition_info(cursor, full_name: str) -> Dict[str, Any]:
    """
    Partition info for the clustered/heap: is_partitioned, scheme, function, key column,
    and total partition_count.
    """
    sql = """
    DECLARE @obj_id int = OBJECT_ID(?);

    WITH p AS (
        SELECT
            object_id,
            index_id,
            COUNT(DISTINCT partition_number) AS partition_count
        FROM sys.partitions
        WHERE object_id = @obj_id
          AND index_id IN (0,1)
        GROUP BY object_id, index_id
    )
    SELECT TOP (1)
        p.partition_count,
        CASE WHEN p.partition_count > 1 THEN 1 ELSE 0 END AS is_partitioned,
        ps.name AS partition_scheme,
        pf.name AS partition_function,
        c.name  AS partition_key
    FROM p
    JOIN sys.indexes i
        ON i.object_id = p.object_id
       AND i.index_id  = p.index_id
    LEFT JOIN sys.partition_schemes ps
        ON ps.data_space_id = i.data_space_id
    LEFT JOIN sys.partition_functions pf
        ON pf.function_id = ps.function_id
    LEFT JOIN sys.index_columns ic
        ON ic.object_id = i.object_id
       AND ic.index_id  = i.index_id
       AND ic.partition_ordinal = 1
    LEFT JOIN sys.columns c
        ON c.object_id = ic.object_id
       AND c.column_id = ic.column_id;
    """
    cursor.execute(sql, (full_name,))
    row = cursor.fetchone()
    if not row:
        return {
            "partition_count": False,
            "is_partitioned": False,
            "partition_scheme": None,
            "partition_function": None,
            "partition_key": None,
        }
    return {
        "partition_count": int(row.partition_count or 0),
        "is_partitioned": bool(row.is_partitioned),
        "partition_scheme": row.partition_scheme,
        "partition_function": row.partition_function,
        "partition_key": row.partition_key,
    }



# ------------- PROCEDURE helpers -----------------


def _get_object_definition(cursor, full_name: str) -> Optional[str]:
    sql = """
    SELECT sm.definition
    FROM sys.sql_modules sm
    WHERE sm.object_id = OBJECT_ID(?);
    """
    cursor.execute(sql, (full_name,))
    row = cursor.fetchone()
    return row.definition if row else None


def _get_procedure_parameters(cursor, full_name: str) -> List[Dict[str, Any]]:
    sql = """
    SELECT
        p.name,
        TYPE_NAME(p.user_type_id) AS data_type,
        p.is_output,
        p.max_length,
        p.precision,
        p.scale
    FROM sys.parameters p
    WHERE p.object_id = OBJECT_ID(?)
    ORDER BY p.parameter_id;
    """
    cursor.execute(sql, (full_name,))
    rows = cursor.fetchall() or []
    params: List[Dict[str, Any]] = []
    for r in rows:
        params.append(
            {
                "name": r.name,
                "data_type": r.data_type,
                "is_output": bool(r.is_output),
                "max_length": r.max_length,
                "precision": r.precision,
                "scale": r.scale,
            }
        )
    return params


def _get_procedure_flags_from_definition(defn: Optional[str]) -> Dict[str, bool]:
    """
    Very simple heuristic flags based on definition text.
    Good enough for dashboard.
    """
    if not defn:
        return {
            "uses_temp_tables": False,
            "uses_transactions": False,
            "uses_cursor": False,
            "uses_while_loop": False,
            "uses_triggers": False,
        }

    text = defn.lower()

    return {
        "uses_temp_tables": "#" in text or "tempdb.." in text,
        "uses_transactions": "begin tran" in text or "commit tran" in text or "rollback tran" in text,
        "uses_cursor": " cursor " in text,
        "uses_while_loop": " while " in text,
        "uses_triggers": " insert " in text and " deleted " in text or " trigger " in text,
    }


def _get_procedure_usage(cursor, full_name: str) -> Dict[str, Any]:
    """
    Last execution time + execution_count from sys.dm_exec_procedure_stats.
    """
    sql = """
    SELECT
        cast(ps.last_execution_time as datetime2(0)) as last_execution_time,
        ps.execution_count
    FROM sys.dm_exec_procedure_stats AS ps
    WHERE ps.object_id  = OBJECT_ID(?)
      AND ps.database_id = DB_ID();
    """
    cursor.execute(sql, (full_name,))
    row = cursor.fetchone()
    if not row:
        return {"last_execution_time": None, "execution_count": None}
    return {
        "last_execution_time": row.last_execution_time,
        "execution_count": int(row.execution_count or 0),
    }

from typing import List, Dict, Any  # at top if not already there

def _get_procedure_exec_history(cursor, full_name: str) -> List[Dict[str, Any]]:
    """
    DMV-based pseudo 'history' for a stored procedure.

    Uses sys.dm_exec_procedure_stats to return a SINGLE aggregate row
    as a list[dict], so the UI can render it as a table.

    Columns we expose (per row):
      - last_execution_time
      - execution_count
      - avg_duration_ms
      - last_duration_ms
      - avg_cpu_ms
      - last_cpu_ms
      - avg_logical_reads
      - last_logical_reads
    """
    cursor.execute(
        """
        SELECT TOP (1)
            ps.execution_count,
            cast(ps.last_execution_time as datetime2(0)) as last_execution_time,
            ps.total_elapsed_time,      -- microseconds
            ps.total_worker_time,       -- microseconds (CPU)
            ps.total_logical_reads,
            ps.total_logical_writes,
            ps.last_elapsed_time,       -- microseconds
            ps.last_worker_time,        -- microseconds
            ps.last_logical_reads,
            ps.last_logical_writes
        FROM sys.dm_exec_procedure_stats AS ps
        WHERE ps.object_id  = OBJECT_ID(?)
          AND ps.database_id = DB_ID()
        ORDER BY ps.last_execution_time DESC;
        """,
        (full_name,),
    )
    row = cursor.fetchone()
    if not row:
        return []

    exec_count = int(row.execution_count or 0)

    def micros_to_ms(val):
        if val is None:
            return None
        return float(val) / 1_000_000.0

    total_elapsed_ms = micros_to_ms(row.total_elapsed_time)
    total_cpu_ms = micros_to_ms(row.total_worker_time)

    avg_duration_ms = (
        total_elapsed_ms / exec_count if exec_count and total_elapsed_ms is not None else None
    )
    avg_cpu_ms = (
        total_cpu_ms / exec_count if exec_count and total_cpu_ms is not None else None
    )

    avg_logical_reads = (
        (row.total_logical_reads or 0) / exec_count if exec_count else None
    )

    history_row = {
        "last_execution_time": row.last_execution_time,
        "execution_count": exec_count,
        "avg_duration_ms": avg_duration_ms,
        "last_duration_ms": micros_to_ms(row.last_elapsed_time),
        "avg_cpu_ms": avg_cpu_ms,
        "last_cpu_ms": micros_to_ms(row.last_worker_time),
        "avg_logical_reads": avg_logical_reads,
        "last_logical_reads": int(row.last_logical_reads or 0),
    }

    # Return as a list so template can loop
    return [history_row]



def _parse_exec_plan_xml(plan_xml: str) -> List[Dict[str, Any]]:
    """
    Parse SQL Server showplan XML into a flat list of RelOp nodes with levels,
    so the UI can render an indented tree.

    Returns:
      [
        {
          "node_id": str,
          "physical_op": str,
          "logical_op": str,
          "estimate_rows": float | None,
          "estimate_io": float | None,
          "estimate_cpu": float | None,
          "subtree_cost": float | None,
          "level": int,          # indentation level for display
        },
        ...
      ]
    """
    if not plan_xml:
        return []

    try:
        root = ET.fromstring(plan_xml)
    except Exception:
        return []

    nodes: List[Dict[str, Any]] = []

    def is_relop(elem) -> bool:
        tag = elem.tag
        return tag.endswith("RelOp")

    def get_attr(elem, name: str) -> Optional[str]:
        return elem.attrib.get(name)

    def parse_float(val: Optional[str]) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def walk_relop(elem, level: int):
        if not is_relop(elem):
            return

        node = {
            "node_id": get_attr(elem, "NodeId"),
            "physical_op": get_attr(elem, "PhysicalOp"),
            "logical_op": get_attr(elem, "LogicalOp"),
            "estimate_rows": parse_float(get_attr(elem, "EstimateRows")),
            "estimate_io": parse_float(get_attr(elem, "EstimateIO")),
            "estimate_cpu": parse_float(get_attr(elem, "EstimateCPU")),
            "subtree_cost": parse_float(get_attr(elem, "EstimatedTotalSubtreeCost")),
            "level": level,
        }
        nodes.append(node)

        # Direct children: look through descendants one level down
        for child in elem:
            if is_relop(child):
                walk_relop(child, level + 1)
            else:
                # some plans nest RelOp deeper inside sub-elements
                for gc in child:
                    if is_relop(gc):
                        walk_relop(gc, level + 1)

    # Some plans have multiple statements; walk all RelOp roots
    for relop in root.iter():
        if is_relop(relop) and get_attr(relop, "NodeId") == "0":
            # treat NodeId = 0 as root(s)
            walk_relop(relop, 0)

    # Fallback: if we didn't find NodeId=0, just walk all RelOp elements
    if not nodes:
        for relop in root.iter():
            if is_relop(relop):
                walk_relop(relop, 0)
                break

    return nodes



def _get_procedure_exec_stats_and_plan(cursor, full_name: str) -> Dict[str, Any]:
    """
    Use DMVs (sys.dm_exec_procedure_stats + sys.dm_exec_query_plan) to get:
      - aggregated execution statistics
      - current cached execution plan XML (if any)

    Returns:
      {
        "has_data": bool,
        "stats": {
            "execution_count": int,
            "last_execution_time": datetime | None,
            "total_elapsed_ms": float | None,
            "total_cpu_ms": float | None,
            "avg_elapsed_ms": float | None,
            "avg_cpu_ms": float | None,
            "last_elapsed_ms": float | None,
            "last_cpu_ms": float | None,
            "total_logical_reads": int | None,
            "total_logical_writes": int | None,
            "last_logical_reads": int | None,
            "last_logical_writes": int | None,
        },
        "plan_xml": str | None
      }
    """
    result: Dict[str, Any] = {
        "has_data": False,
        "stats": None,
        "plan_xml": None,
    }

    # 1) Get stats + plan_handle from dm_exec_procedure_stats
    cursor.execute(
        """
        SELECT TOP (1)
            ps.execution_count,
            cast(ps.last_execution_time as datetime2(0)) as last_execution_time,
            ps.total_elapsed_time,      -- microseconds
            ps.total_worker_time,       -- microseconds (CPU)
            ps.total_logical_reads,
            ps.total_logical_writes,
            ps.last_elapsed_time,       -- microseconds
            ps.last_worker_time,        -- microseconds
            ps.last_logical_reads,
            ps.last_logical_writes,
            ps.plan_handle
        FROM sys.dm_exec_procedure_stats AS ps
        WHERE ps.object_id  = OBJECT_ID(?)
          AND ps.database_id = DB_ID()
        ORDER BY ps.last_execution_time DESC;
        """,
        (full_name,),
    )
    s = cursor.fetchone()
    if not s:
        return result  # no stats in cache

    exec_count = int(s.execution_count or 0)

    def micros_to_ms(val):
        if val is None:
            return None
        return float(val) / 1000.0

    total_elapsed_ms = micros_to_ms(s.total_elapsed_time)
    total_cpu_ms = micros_to_ms(s.total_worker_time)
    avg_elapsed_ms = (
        total_elapsed_ms / exec_count if exec_count and total_elapsed_ms is not None else None
    )
    avg_cpu_ms = (
        total_cpu_ms / exec_count if exec_count and total_cpu_ms is not None else None
    )

    stats = {
        "execution_count": exec_count,
        "last_execution_time": s.last_execution_time,
        "total_elapsed_ms": total_elapsed_ms,
        "total_cpu_ms": total_cpu_ms,
        "avg_elapsed_ms": avg_elapsed_ms,
        "avg_cpu_ms": avg_cpu_ms,
        "last_elapsed_ms": micros_to_ms(s.last_elapsed_time),
        "last_cpu_ms": micros_to_ms(s.last_worker_time),
        "total_logical_reads": int(s.total_logical_reads or 0),
        "total_logical_writes": int(s.total_logical_writes or 0),
        "last_logical_reads": int(s.last_logical_reads or 0),
        "last_logical_writes": int(s.last_logical_writes or 0),
    }

    result["has_data"] = True
    result["stats"] = stats

    # 2) Get plan XML from dm_exec_query_plan using plan_handle
    plan_xml = None
    try:
        cursor.execute(
            "SELECT query_plan FROM sys.dm_exec_query_plan(?);",
            (s.plan_handle,),
        )
        p = cursor.fetchone()
        if p and getattr(p, "query_plan", None):
            plan_xml = p.query_plan
    except pyodbc.Error:
        plan_xml = None

    result["plan_xml"] = plan_xml
    return result



# ------------- VIEW helpers -----------------


def _get_view_flags_from_definition(defn: Optional[str]) -> Dict[str, Any]:
    if not defn:
        return {
            "is_indexed_view": False,
            "is_schema_bound": False,
            "uses_cursor": False,
            "uses_while_loop": False,
            "uses_triggers": False,
        }
    text = defn.lower()
    return {
        "is_indexed_view": "create unique clustered index" in text,
        "is_schema_bound": "with schemabinding" in text,
        "uses_cursor": " cursor " in text,
        "uses_while_loop": " while " in text,
        "uses_triggers": " trigger " in text,
    }


# ------------- FUNCTION helpers -----------------


def _get_function_extra(cursor, full_name: str) -> Dict[str, Any]:
    """
    Function type + return data type.
    """
    sql = """
    SELECT
        o.type_desc,
        rt.name AS return_type
    FROM sys.objects o
    LEFT JOIN sys.types rt
        ON o.type IN ('FN','TF','IF')  -- scalar or table-valued
       AND rt.user_type_id = o.type
    WHERE o.object_id = OBJECT_ID(?);
    """
    # Note: return type via sys.types is tricky; we fallback to parsing definition.
    cursor.execute(sql, (full_name,))
    row = cursor.fetchone()
    function_type = row.type_desc if row else None

    # Fallback: rough parse from definition
    defn = _get_object_definition(cursor, full_name)
    return_type = None
    if defn:
        text = defn.lower()
        # crude heuristic: look for "returns @something TABLE" or "RETURNS datatype"
        if "returns table" in text:
            return_type = "TABLE"
        elif "returns" in text:
            # this is intentionally simple; real parsing is ugly
            return_type = "SCALAR"

    return {
        "function_type": function_type,
        "return_data_type": return_type,
    }


# ------------- dependency helpers -----------------

def _map_type_desc_to_logical(type_desc: str) -> str:
    """
    Map sys.objects.type_desc to logical type for the UI.
    """
    td = (type_desc or "").upper()
    if "TABLE" in td:
        return "TABLE"
    if "VIEW" in td:
        return "VIEW"
    if "PROCEDURE" in td:
        return "PROCEDURE"
    if "FUNCTION" in td:
        return "FUNCTION"
    return "OTHER"


def _get_referenced_objects(cursor, object_id: int) -> List[Dict[str, Any]]:
    """
    Objects THIS object depends on (objects referenced by this).
    """
    sql = """
    SELECT DISTINCT
        s2.name AS schema_name,
        o2.name AS object_name,
        o2.type_desc
    FROM sys.sql_expression_dependencies AS d
    INNER JOIN sys.objects AS o2
        ON d.referenced_id = o2.object_id
    INNER JOIN sys.schemas AS s2
        ON o2.schema_id = s2.schema_id
    WHERE d.referencing_id = ?
      AND d.referenced_id IS NOT NULL;
    """
    cursor.execute(sql, (object_id,))
    rows = cursor.fetchall() or []

    results: List[Dict[str, Any]] = []
    for r in rows:
        logical_type = _map_type_desc_to_logical(r.type_desc)
        results.append(
            {
                "full_name": f"{r.schema_name}.{r.object_name}",
                "object_type": logical_type,   # used by template for ?type=
                "type_desc": r.type_desc,
            }
        )
    return results


def _get_referencing_objects(cursor, object_id: int) -> List[Dict[str, Any]]:
    """
    Objects that depend on THIS object (objects that reference this).
    """
    sql = """
    SELECT DISTINCT
        s2.name AS schema_name,
        o2.name AS object_name,
        o2.type_desc
    FROM sys.sql_expression_dependencies AS d
    INNER JOIN sys.objects AS o2
        ON d.referencing_id = o2.object_id
    INNER JOIN sys.schemas AS s2
        ON o2.schema_id = s2.schema_id
    WHERE d.referenced_id = ?;
    """
    cursor.execute(sql, (object_id,))
    rows = cursor.fetchall() or []

    results: List[Dict[str, Any]] = []
    for r in rows:
        logical_type = _map_type_desc_to_logical(r.type_desc)
        results.append(
            {
                "full_name": f"{r.schema_name}.{r.object_name}",
                "object_type": logical_type,   # used by template for ?type=
                "type_desc": r.type_desc,
            }
        )
    return results

# ------------- main entry point -----------------


def get_object_metadata(
    conn: pyodbc.Connection,
    database_name: str,
    full_name: str,
    obj_type: str,
) -> Optional[Dict[str, Any]]:
    """
    Main function used by your Flask layer.

    conn          : pyodbc connection to the SQL Server instance
    database_name : name of the DB containing the object
    full_name     : "schema.object" (schema optional, defaults to dbo)
    obj_type      : "table", "view", "procedure", "function" (any case)
    """
    logical_type = _normalize_object_type(obj_type)
    schema_name, object_name = _split_full_name(full_name)

    cursor = conn.cursor()
    # Make sure we're in the right DB
    cursor.execute(f"USE [{database_name}];")

    base = _get_basic_metadata(cursor, schema_name, object_name, logical_type)
    if not base:
        return None

    meta: Dict[str, Any] = {
        "schema_name": base["schema_name"],
        "object_name": base["object_name"],
        "full_name": f"{base['schema_name']}.{base['object_name']}",
        "type": logical_type,
        "type_desc": base["type_desc"],
        "create_date": base["create_date"],
        "modify_date": base["modify_date"],
        "extras": {},
        "referenced": [],
        "referencing": [],
    }

    extras: Dict[str, Any] = {}

    if logical_type == "TABLE":
        # 1) base space info
        base_space = _get_table_space_basic(cursor, meta["full_name"])
        extras["row_count"] = base_space["row_count"]
        extras["reserved_mb"] = base_space["reserved_mb"]
        extras["used_mb"] = base_space["used_mb"]

        # 2) per-index info
        indexes = _get_table_indexes(cursor, meta["full_name"])
        extras["indexes"] = indexes
        extras["index_count"] = len(indexes)

        # 3) partition info
        pinfo = _get_table_partition_info(cursor, meta["full_name"])
        extras["is_partitioned"] = pinfo["is_partitioned"]
        extras["partition_scheme"] = pinfo["partition_scheme"]
        extras["partition_function"] = pinfo["partition_function"]
        extras["partition_key"] = pinfo["partition_key"]
        extras["partition_count"] = pinfo["partition_count"]


    elif logical_type == "PROCEDURE":
        defn = _get_object_definition(cursor, meta["full_name"])
        flags = _get_procedure_flags_from_definition(defn)
        params = _get_procedure_parameters(cursor, meta["full_name"])
        usage = _get_procedure_usage(cursor, meta["full_name"])
        history = _get_procedure_exec_history(cursor, meta["full_name"])

        extras.update(flags)
        extras["parameters"] = params
        extras["last_execution_time"] = usage["last_execution_time"]
        extras["execution_count"] = usage["execution_count"]
        extras["exec_history"] = history

        dmv_info = _get_procedure_exec_stats_and_plan(cursor, meta["full_name"])

        if dmv_info.get("has_data"):
            stats = dmv_info["stats"] or {}
            # Backward-compatible simple fields
            extras["execution_count"] = stats.get("execution_count")
            extras["last_execution_time"] = stats.get("last_execution_time")

            # Rich stats for UI later if you want
            extras["exec_stats"] = stats
        else:
            extras["execution_count"] = None
            extras["last_execution_time"] = None
            extras["exec_stats"] = None

        # 3) Plan XML + parsed operators for graphical view
        plan_xml = dmv_info.get("plan_xml")
        extras["exec_plan_xml"] = plan_xml

        if plan_xml:
            extras["exec_plan_ops"] = _parse_exec_plan_xml(plan_xml)
        else:
            extras["exec_plan_ops"] = []

        

    elif logical_type == "VIEW":
        defn = _get_object_definition(cursor, meta["full_name"])
        flags = _get_view_flags_from_definition(defn)
        extras.update(flags)

    elif logical_type == "FUNCTION":
        fextra = _get_function_extra(cursor, meta["full_name"])
        extras.update(fextra)

    meta["extras"] = extras

    # Dependencies
        # Dependencies  (need both object_id and full_name for fallbacks)
    meta["referenced"] = _get_referenced_objects(cursor, base["object_id"])
    meta["referencing"] = _get_referencing_objects(cursor, base["object_id"])


    return meta
