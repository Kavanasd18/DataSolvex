import os
from datetime import date, datetime, timedelta
 
import pyodbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
 
 
from dash import Dash, html, dcc
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dotenv import load_dotenv
 
 
# --------------------------------------------------
# ENV
# --------------------------------------------------
load_dotenv()
 
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_TRUSTED = os.getenv("DB_TRUSTED", "YES").upper() == "YES"
 
 
def get_connection():
    """Return a pyodbc connection to SQL Server."""
    if DB_TRUSTED:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            "Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};PWD={DB_PASSWORD};"
        )
    return pyodbc.connect(conn_str)
 
 
# --------------------------------------------------
# PAGE -> MODULE MAPPING (DISPLAY-ONLY)
#
# IMPORTANT:
# - We still INSERT *all* raw pages into dbo.page_access_logs.
# - In Log Analytics (this Dash app), we *DISPLAY* only coarse module names
#   so the UI isn't polluted by per-object URLs and Dash asset bundle hits.
# --------------------------------------------------

# Hard ignores: show nothing / skip from analytics.
# (We don't want Dash component bundles to become "top pages".)
IGNORE_PREFIXES = (
    "/loganalytics/_dash-component-suites/",
)

IGNORE_EXTENSIONS = (
    ".js",
    ".css",
    ".map",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".ico",
)


# Exact pages (simple static routes)
PAGE_TO_MODULE = {
    # --- Landing / Server list ---

    "/index": "index",
    "/index.html": "index",

    # --- Login ---
    "/login": "login",
    "/login.html": "login",

    # --- Log analytics page itself ---
    "/loganalytics": "loganalytics",
    "/loganalytics/": "loganalytics",

    # --- SQL Server main tools hub ---
    "/sqlserver_main": "sql_server_main",
    "/sqlserver_main.html": "sql_server_main",

    # --- DB Refresh (main + staging are inside this tool) ---
    "/dbrefresh": "dbrefresh",
    "/dbrefresh.html": "dbrefresh",
    "/dbpage": "dbrefresh",
    "/dbpage.html": "dbrefresh",
    "/database_page": "dbrefresh",
    "/database_page.html": "dbrefresh",

    # --- Login Creation ---
    "/form": "login creation",
    "/form.html": "login creation",

    # --- Replication dashboard (summary + detail) ---
    "/replication_dashboard": "replication dashboard",
    "/replication_dashboard/summary": "replication dashboard",
    "/replication_dashboard.html": "replication dashboard",
    "/replication_dashboard/summary.html": "replication dashboard",

    # If you still have the older /dashboard routes:
    "/dashboard": "replication dashboard",
    "/dashboard/summary": "replication dashboard",
    "/dashboard.html": "replication dashboard",
    "/dashboard/summary.html": "replication dashboard",

    # --- Inventory landing route (you have /inventory too) ---
    "/inventory": "inventory dashboard",
    "/": "inventory dashboard",
    "/server/" : "inventory dashboard",
    "/environments.html" : "inventory dashboard",

    #---userclone --
    "/userclone/logging_defaults":"user clone",

    #--- replication reinit
    "/replication.html" : "replication reinit",
}


OVERALL_ALLOWED_MODULES = {
    "sql_server_main",
    "dbrefresh",
    "replication dashboard",
    "login creation",
    "login",
    "replication reinit",
    "inventory dashboard",
    "user clone",
    "ssis",
    "index",
}


PLOT_FONT = '"Inter", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif'

def apply_plot_font(fig):
    fig.update_layout(font=dict(family=PLOT_FONT))
    return fig




def map_page_to_module(page: str) -> str | None:
    """Return a coarse module name for analytics UI.

    Returns:
      - None   -> ignore this row in the Dash analytics (e.g., JS bundles)
      - string -> module name to show
    """

    p = str(page).split("?", 1)[0].strip().lower()

    # Ignore Dash bundle noise (but keep it inserted in DB)
    for pref in IGNORE_PREFIXES:
        if p.startswith(pref):
            return None

    for ext in IGNORE_EXTENSIONS:
        if p.endswith(ext):
            return None

    # 1) exact mapping first
    if p in PAGE_TO_MODULE:
        return PAGE_TO_MODULE[p]

    # 2) pattern mapping (dynamic URLs)
    # Replication Reinitialization tool (Blueprint under /replication/...)
    if p.startswith("/replication/"):
        return "replication reinit"
    
    if p.startswith("/replication."):
        return "replication reinit"

    # Inventory Dashboard (server/db/object drilldowns)
    if p.startswith("/server/"):
        return "inventory dashboard"

    # User Clone tool
    if p.startswith("/userclone"):
        return "user clone"

    # SSIS tool
    if p.startswith("/ssis"):
        return "ssis"
    
    # Log analytics (Dash app area)
    if p.startswith("/loganalytics"):
        return "loganalytics"

    # Inventory tool (dynamic pages)
    if p.startswith("/inventory"):
        return "inventory dashboard"


 
 
# --------------------------------------------------
# FILTER METADATA
# --------------------------------------------------
def load_filter_options():
    with get_connection() as conn:
        pages = pd.read_sql("SELECT DISTINCT page FROM dbo.page_access_logs;", conn)
        date_bounds = pd.read_sql(
            """
            SELECT
                MIN(enter_time) AS min_enter,
                MAX(exit_time)  AS max_exit
            FROM dbo.page_access_logs;
            """,
            conn,
        )
 
    modules_set = set()
    for p in pages["page"].dropna():
        m = map_page_to_module(p)
        if m:  # m can be None for ignored assets
            modules_set.add(m)

    modules_set = modules_set.intersection(OVERALL_ALLOWED_MODULES)
    modules_list = sorted(modules_set)

 
    min_enter = date_bounds["min_enter"].iloc[0]
    max_exit = date_bounds["max_exit"].iloc[0]
    if pd.isna(min_enter) or pd.isna(max_exit):
        today = datetime.now().date()
        return modules_list, today, today
 
    min_date = pd.to_datetime(min_enter).date()
    max_date = pd.to_datetime(max_exit).date()
    return modules_list, min_date, max_date
 
 
# --------------------------------------------------
# CORE DATA LOAD
# --------------------------------------------------
def load_filtered_data(start_dt, end_dt, module_name, login_contains, ip_contains):
    conditions = []
    params = []

    if start_dt:
        conditions.append("enter_time >= ?")
        params.append(start_dt)
    if end_dt:
        conditions.append("exit_time <= ?")
        params.append(end_dt)

    if login_contains:
        conditions.append("login_name LIKE ?")
        params.append(f"%{login_contains}%")

    if ip_contains:
        conditions.append("ip_address LIKE ?")
        params.append(f"%{ip_contains}%")

    where_clause = " AND ".join(conditions) or "1=1"

    sql = f"""
        SELECT
            login_name,
            ip_address AS client_ip,
            page,
            enter_time,
            exit_time,
            duration_seconds,
            CAST(enter_time AS date) AS log_date
        FROM dbo.page_access_logs
        WHERE {where_clause};
    """

    with get_connection() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if not df.empty:
        df["module_name"] = df["page"].apply(map_page_to_module)
        df = df[df["module_name"].notna()]
    else:
        df["module_name"] = pd.Series(dtype=str)
        return df

    # Exclude anonymous users (case-insensitive)
    df = df[~df["login_name"].fillna("").str.strip().str.lower().isin(["anonymous", "anon"])]

    # ✅ NEW LOGIC:
    if module_name == "DataSolveX (overall)":
        # Only show these modules in overall
        df = df[df["module_name"].isin(OVERALL_ALLOWED_MODULES)]
    else:
        # Normal behavior: filter by selected module
        if module_name:
            df = df[df["module_name"] == module_name]

    return df
 
 
# --------------------------------------------------
# THEME COLORS (Blue-led + muted accents; NOT bright)
# --------------------------------------------------
NAVY = "#1A237E"
PRIMARY = "#0D47A1"
BLUE_1 = "#1565C0"
BLUE_2 = "#1976D2"
BLUE_3 = "#1E88E5"
 
SLATE_1 = "#455A64"
SLATE_2 = "#546E7A"
SLATE_3 = "#37474F"
 
INDIGO_1 = "#4E5D94"
INDIGO_2 = "#5C6BC0"
 
TEAL_DARK = "#006064"  # muted, not bright
 
CAT_SERIES = [PRIMARY, BLUE_1, BLUE_2, SLATE_1, SLATE_2, INDIGO_1, INDIGO_2, BLUE_3, SLATE_3, TEAL_DARK]
GRID = "rgba(13,71,161,0.08)"
AXIS = "rgba(26,35,126,0.18)"
 
 
# --------------------------------------------------
# FIGURE NORMALIZATION (compact + readable + theme)
# --------------------------------------------------
def apply_theme(fig, height=380, legend="none", colorway=None):
    if legend == "h":
        legend_cfg = dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10, color=NAVY),
        )
    elif legend == "v":
        legend_cfg = dict(
            orientation="v",
            yanchor="top",
            y=1.0,
            xanchor="right",
            x=1.0,
            bgcolor="rgba(255,255,255,0.90)",
            bordercolor="rgba(26,35,126,0.18)",
            borderwidth=1,
            font=dict(size=10, color=NAVY),
        )
    else:
        legend_cfg = dict(visible=False)
 
    fig.update_layout(
        template="plotly_white",
        height=height,
        autosize=True,
        margin=dict(l=10, r=10, t=44, b=10),
        font=dict(size=11, color=NAVY),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        legend=legend_cfg,
        colorway=colorway or CAT_SERIES,
        uniformtext=dict(minsize=8, mode="show"),  # prevent text clutter in small bars
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=GRID,
        zeroline=False,
        linecolor=AXIS,
        tickfont=dict(color=NAVY),
        automargin=True,
        title_font=dict(color=NAVY, size=12),
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=GRID,
        zeroline=False,
        linecolor=AXIS,
        tickfont=dict(color=NAVY),
        automargin=True,
        title_font=dict(color=NAVY, size=12),
    )
    return fig
 
 
def empty_fig(h=360):
    fig = px.line()
    fig = apply_theme(fig, height=h, legend="none", colorway=[BLUE_1])
    fig.add_annotation(
        text="No data for selected filters",
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font=dict(size=13, color="rgba(26,35,126,0.70)"),
    )
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return fig
 
 
# --------------------------------------------------
# UI HELPERS
# --------------------------------------------------
def _tile(label, id_, gradient):
    return html.Div(
        style={
            "borderRadius": "12px",
            "padding": "14px 16px",
            "color": "#ffffff",
            "background": gradient,
            "boxShadow": "0 10px 18px rgba(2, 6, 23, 0.10)",
            "minHeight": "78px",
            "display": "flex",
            "flexDirection": "column",
            "justifyContent": "center",
        },
        children=[
            html.Div(label, style={"fontSize": "0.78rem", "opacity": 0.95, "fontWeight": "900"}),
            html.Div(id=id_, style={"fontSize": "1.55rem", "fontWeight": "950", "marginTop": "6px"}),
        ],
    )
 
 
def _card(title, graph_id, height=380, title_id=None):
    return html.Div(
        style={
            "backgroundColor": "#ffffff",
            "borderRadius": "12px",
            "border": "1px solid rgba(26,35,126,0.12)",
            "padding": "10px",
            "boxShadow": "0 10px 18px rgba(2,6,23,0.06)",
            "overflow": "hidden",
        },
        children=[
            html.Div(
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "margin": "4px 6px 8px",
                    "gap": "10px",
                },
                children=[
                    html.Div(
                        title,
                        id=title_id,
                        n_clicks=0,
                        style={
                            "fontSize": "0.92rem",
                            "fontWeight": "950",
                            "color": NAVY,
                            "cursor": "pointer",
                            "userSelect": "none",
                            "flex": "1",
                            "minWidth": "0",
                            "whiteSpace": "nowrap",
                            "overflow": "hidden",
                            "textOverflow": "ellipsis",
                        },
                    ),
                    html.Div(
                        "⤢",
                        id=f"{title_id}-fs",
                        n_clicks=0,
                        title="Open fullscreen",
                        style={
                            "fontSize": "1rem",
                            "cursor": "pointer",
                            "color": NAVY,
                            "opacity": 0.85,
                            "flex": "0 0 auto",
                        },
                    ),
                ],
            ),
            dcc.Graph(
                id=graph_id,
                figure={},
                config={"displayModeBar": False, "responsive": True},
                style={"height": f"{height}px", "width": "100%"},
            ),
        ],
    )
 
 
# --------------------------------------------------
# DASH APP FACTORY
# --------------------------------------------------
def create_log_dash(flask_app):
    try:
        modules_list, min_date, max_date = load_filter_options()
    except Exception as e:
        print(f"Warning: log_dash.load_filter_options failed: {e}")
        today = datetime.now().date()
        modules_list = []
        min_date = today
        max_date = today
 
    dash_app = Dash(
        __name__,
        server=flask_app,
        url_base_pathname="/loganalytics/",
        suppress_callback_exceptions=True,
    )
    dash_app.title = "Usage Analytics Dashboard"
 
    dash_app.index_string = f"""
    <!DOCTYPE html>
    <html>
    <head>
        {{%metas%}}
        <title>{{%title%}}</title>
        {{%favicon%}}
        {{%css%}}
 
        <!-- IMPORTANT: match your flask static path -->
        <link rel="stylesheet" href="/static/global.css">
 
        <style>
          html, body {{ height: 100%; margin: 0; }}
          body {{ background: linear-gradient(180deg, #eef6ff 0%, #e8f2ff 40%, #eef6ff 100%); }}
 
          .dash-graph {{ width: 100% !important; }}
          .dash-graph > div {{ width: 100% !important; }}
          .js-plotly-plot, .plot-container {{
            width: 100% !important;
            max-width: 100% !important;
            overflow: hidden !important;
          }}
          .js-plotly-plot .main-svg {{ max-width: 100% !important; }}
 
          .top-fixed {{
            position: fixed;
            top: 0; left: 0; right: 0;
            z-index: 99999;
            box-shadow: 0 12px 22px rgba(2,6,23,0.18);
          }}
 
          .brandbar{{
            background:#ffffff;
            padding: 0; /* important */
            border-bottom: 1px solid #e1e1e1;
        }}
          .brandbar-inner{{
            max-width: 1400px;          /* SAME as .app-nav-container */
            margin: 0 auto;
            padding: 0 24px;            /* SAME as .app-nav-container */
            height: 48px;               /* SAME as .app-nav-container */
            display:flex;
            align-items:center;
            justify-content:flex-start;
            }}

          .crumbbar-inner{{
            max-width: 1400px;          /* SAME as .app-breadcrumb-container */
            margin: 0 auto;
            padding: 0 24px;            /* SAME as .app-breadcrumb-container */
            height: 36px;               /* SAME as .app-breadcrumb-container */
            display:flex;
            align-items:center;
            justify-content:space-between;
            gap: 14px;
            }}
          .brandtext{{
            font-size: 1.25rem;
            font-weight: 700;
            background: linear-gradient(135deg, #03045e 0%, #0077b6 100%);
            -webkit-background-clip: text;
            background-clip: text;
            -webkit-text-fill-color: transparent;
            text-decoration: none;
            letter-spacing: 0.2px;
            }}
 
          .crumbbar {{
            background: #023e8a;        /* SAME as var(--french-blue) */
            padding: 0;                 /* important */
            border-bottom: 1px solid rgba(255,255,255,0.10);
          }}
          
          .crumbs {{
            color: #ffffff;
            font-weight: 600;
            font-size: 0.875rem;
            display:flex;
            align-items:center;
            gap: 10px;
            white-space: nowrap;
          }}
          .crumbs a {{
            color: #ffffff;
            text-decoration: none;
            font-weight: 600;
          }}
          .crumbsep{{ opacity: 0.85; }}

          .crumbs a:hover{{
            text-decoration: underline;
            }}
 
          .btn-refresh {{
            background: rgba(255,255,255,0.14);
            border: 1px solid rgba(255,255,255,0.40);
            color: #ffffff;
            padding: 6px 12px;
            border-radius: 10px;
            font-weight: 950;
            cursor: pointer;
          }}
 
          /* space for sticky header */
          .top-spacer{{ height: 0px; }}
 
          .shell {{
            max-width: 96vw;
            width: 96vw;
            margin: 0 auto;
            padding: 14px 18px 26px;
          }}
 
          .filters {{
            background: #ffffff;
            border: 1px solid rgba(26,35,126,0.10);
            border-radius: 12px;
            padding: 12px;
            box-shadow: 0 10px 18px rgba(2,6,23,0.06);
            overflow: visible !important; /* allow calendar dropdown */
          }}
 
          .filters-grid {{
            display: grid;
            grid-template-columns: 1.2fr 1fr 1fr 1fr 1fr;
            gap: 12px;
            overflow: visible !important;
          }}
          @media (max-width: 980px) {{
            .filters-grid {{ grid-template-columns: 1fr 1fr; }}
          }}
 
          .filters-grid > div {{
            display: flex;
            flex-direction: column;
            gap: 6px;
            min-width: 0;
          }}
 
          .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 12px;
            margin-top: 12px;
          }}
          @media (max-width: 980px) {{
            .kpi-grid {{ grid-template-columns: 1fr 1fr; }}
          }}
 
          .charts-grid-3 {{
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 12px;
            margin-top: 14px;
          }}
 
          .charts-grid-2 {{
            display: grid;
            grid-template-columns: minmax(0, 2fr) minmax(0, 1fr);
            gap: 12px;
            margin-top: 14px;
          }}
 
          @media (max-width: 980px) {{
            .charts-grid-3 {{ grid-template-columns: 1fr; }}
            .charts-grid-2 {{ grid-template-columns: 1fr; }}
          }}
 
          /* ==========================================================
             ✅ Calendar popup FIX (NO contradictions, NO "display:none")
             ========================================================== */
          .filters, .filters-grid, .shell {{ overflow: visible !important; }}
          .DateRangePicker_picker {{ z-index: 999999 !important; }}
 
          .DateRangePickerInput {{
            display: inline-flex !important;
            align-items: center !important;
            gap: 8px !important;
            width: 100% !important;
            background: #fff !important;
            border: 1px solid rgba(26,35,126,0.14) !important;
            border-radius: 10px !important;
            padding: 4px 8px !important;
          }}
 
          .DateInput {{ width: 130px !important; }}
          .DateInput_input {{
            width: 130px !important;
            font-size: 12px !important;
            padding: 6px 8px !important;
            border: 0 !important;
            box-sizing: border-box !important;
            text-align: left !important;
            color: {NAVY} !important;
          }}
 
          :root {{ --cal-col: 32px; }}
          .DayPicker__horizontal {{ font-size: 12px !important;
            padding-top: 0 !important; }}
         
          .DayPicker {{ position: relative !important; }}
 
          /* Hard reset weekday header so global.css can't ruin it */
          .DayPicker_weekHeaders {{ position: relative !important; margin: 0 !important; padding: 0 !important; }}
          .DayPicker_weekHeader {{ position: relative !important; margin: 0 !important; padding: 0 10px !important; }}
 
          /* Hide weekday header row (Sun Mon Tue...) */
          .DayPicker_weekHeaders,
          .DayPicker_weekHeader {{display: none !important;}}
 
 
          .DayPicker_weekHeader_ul {{
            list-style: none !important;
            margin: 0 !important;
            padding: 0 !important;
            white-space: nowrap !important;
          }}
 
          .DayPicker_weekHeader_ul li {{
            display: inline-block !important;
            width: var(--cal-col) !important;
            height: 22px !important;
            line-height: 22px !important;
            text-align: center !important;
            font-size: 11px !important;
            font-weight: 800 !important;
            color: rgba(26,35,126,0.85) !important;
            margin: 0 !important;
            padding: 0 !important;
            vertical-align: top !important;
          }}
 
          .CalendarMonth_table {{ margin-top: 6px !important; border-collapse: collapse !important; }}
 
          .CalendarMonth_caption {{
            padding: 6px 0 6px !important;
            font-size: 13px !important;
            font-weight: 900 !important;
            color: #1A237E !important;
          }}
 
          .CalendarDay,
          .CalendarDay__default,
          .CalendarDay__selected_span,
          .CalendarDay__selected,
          .CalendarDay__hovered_span {{
              width: var(--cal-col) !important;
              height: var(--cal-col) !important;
              line-height: var(--cal-col) !important;
              font-size: 12px !important;
              text-align: center !important;
              vertical-align: middle !important;
              box-sizing: border-box !important;
          }}
 
          .CalendarDay__default {{ color: #1A237E !important; }}
 
          .CalendarDay__selected,
          .CalendarDay__selected:active,
          .CalendarDay__selected:hover {{
            background: #0D47A1 !important;
            border: 1px solid #0D47A1 !important;
            color: #fff !important;
          }}
 
          .CalendarDay__selected_span {{
            background: rgba(13,71,161,0.18) !important;
            border: 1px solid rgba(13,71,161,0.12) !important;
            color: #1A237E !important;
          }}
 
          .CalendarDay__hovered_span {{
            background: rgba(13,71,161,0.12) !important;
            border: 1px solid rgba(13,71,161,0.10) !important;
            color: #1A237E !important;
          }}
 
          .DayPickerNavigation_button {{ padding: 4px 6px !important; }}
 
          /* ==========================
             Graph modal (click outside to close)
             ========================== */
          #graph-modal-shell {{
            position: fixed;
            inset: 0;
            z-index: 999999;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 18px;
          }}
 
          #graph-modal-backdrop {{
            position: absolute;
            inset: 0;
            background: rgba(2, 6, 23, 0.55);
          }}
 
          #graph-modal-box {{
            position: relative;
            z-index: 2;
            width: min(1200px, 96vw);
            background: #ffffff;
            border-radius: 14px;
            border: 1px solid rgba(26,35,126,0.18);
            box-shadow: 0 18px 40px rgba(2,6,23,0.35);
            padding: 12px;
          }}

          /* ---- Typography alignment with DataSolveX global.css (no internet) ---- */
          :root{{
            --font-sans: "Inter", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;}}
            
          html, body, #react-entry-point, #_dash-app-content, .dsx-root{{
            font-family: var(--font-sans) !important;
            }}
            
            
          button, input, select, textarea{{
            font-family: var(--font-sans) !important;}}
            
          h1,h2,h3,h4,h5,h6, .brandtext, .crumbs{{font-family: var(--font-sans) !important;}}

          .dsx-footer{{
            margin-top: auto; /* pushes footer down in flex layout */
            background: linear-gradient(90deg, #1A237E, #0D47A1);
            color: #ffffff;
            padding: 14px 24px;
            text-align: center;
            font-weight: 0.9rem;            /* match your site (900 looks too heavy) */
            letter-spacing: 0.2px;
            font-family: var(--font-sans) !important;
            }}


        </style>
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
    </html>
    """
 
    dash_app.layout = html.Div(
        children=[
            dcc.Location(id="dash-url", refresh=True),
 
            # store which chart is currently opened in modal
            dcc.Store(id="modal-chart-key", data=""),
 
            # --------------------------------------------------
            # STICKY HEADER + BREADCRUMB
            # --------------------------------------------------
            html.Div(
                className="top-fixed",
                children=[
                    html.Div(
                        className="brandbar",
                        children=html.Div(
                            className="brandbar-inner",
                            children=[html.Div("DataSolveX", className="brandtext")],
                        ),
                    ),
                    html.Div(
                        className="crumbbar",
                        children=html.Div(
                            className="crumbbar-inner",
                            children=[
                                html.Div(
                                    className="crumbs",
                                    children=[
                                        dcc.Link("Login", href="/login", refresh=True),
                                        html.Span("›", className="crumbsep"),
                                        dcc.Link("Admin", href="/admin-home", refresh=True),
                                        html.Span("›", className="crumbsep"),
                                        html.Span("Usage Analytics", style={"textDecoration": "underline"}),
                                    ],
                                ),
                                html.Button("Refresh", id="btn-refresh", n_clicks=0, className="btn-refresh"),
                            ],
                        ),
                    ),
                ],
            ),
 
            html.Div(className="top-spacer"),
 
            # --------------------------------------------------
            # MAIN CONTENT
            # --------------------------------------------------
            html.Div(
                className="shell",
                children=[
                    # FILTERS
                    html.Div(
                        className="filters",
                        children=[
                            html.Div(
                                className="filters-grid",
                                children=[
                                    html.Div(
                                        children=[
                                            html.Label(
                                                "Time range",
                                                style={"fontSize": "0.78rem", "fontWeight": "900", "color": NAVY},
                                            ),
                                            dcc.Dropdown(
                                                id="filter-time-range",
                                                options=[
                                                    {"label": "Last 1 Hour", "value": "1h"},
                                                    {"label": "Last 12 Hours", "value": "12h"},
                                                    {"label": "Last 1 Day", "value": "1d"},
                                                    {"label": "Last 1 Week", "value": "1w"},
                                                    {"label": "Last 1 Month", "value": "1m"},
                                                    {"label": "Custom Range", "value": "custom"},
                                                ],
                                                value="1d",
                                                clearable=False,
                                            ),
                                            html.Div(
                                                id="custom-date-container",
                                                style={"display": "none", "marginTop": "10px"},
                                                children=[
                                                    html.Label(
                                                        "Custom date range",
                                                        style={"fontSize": "0.78rem", "fontWeight": "900", "color": NAVY},
                                                    ),
                                                    dcc.DatePickerRange(
                                                        id="filter-custom-range",
                                                        start_date=min_date,
                                                        end_date=max_date,
                                                        min_date_allowed=min_date,
                                                        max_date_allowed=max_date,
                                                        display_format="YYYY-MM-DD",
                                                        with_portal=False,
                                                        number_of_months_shown=1,
                                                        minimum_nights=0,
                                                        clearable=True,
                                                    ),
                                                ],
                                            ),
                                        ]
                                    ),
                                    html.Div(
                                        children=[
                                            html.Label(
                                                "Module",
                                                style={"fontSize": "0.78rem", "fontWeight": "900", "color": NAVY},
                                            ),
                                            dcc.Dropdown(
                                                id="filter-module",
                                                options=[{"label": "DataSolveX (overall)", "value": "DataSolveX (overall)"}]
                                                + [{"label": m, "value": m} for m in modules_list],
                                                value="DataSolveX (overall)",
                                                clearable=False,
                                            ),
                                        ]
                                    ),
                                    html.Div(
                                        children=[
                                            html.Label(
                                                "Login contains",
                                                style={"fontSize": "0.78rem", "fontWeight": "900", "color": NAVY},
                                            ),
                                            dcc.Input(
                                                id="filter-login",
                                                type="text",
                                                placeholder="e.g. sakshi",
                                                style={
                                                    "width": "80%",
                                                    "padding": "9px 10px",
                                                    "borderRadius": "10px",
                                                    "border": "1px solid rgba(26,35,126,0.14)",
                                                    "marginTop": "4px",
                                                },
                                            ),
                                        ]
                                    ),
                                    html.Div(
                                        children=[
                                            html.Label(
                                                "IP contains",
                                                style={"fontSize": "0.78rem", "fontWeight": "900", "color": NAVY},
                                            ),
                                            dcc.Input(
                                                id="filter-ip",
                                                type="text",
                                                placeholder="e.g. 127.0.0.1",
                                                style={
                                                    "width": "80%",
                                                    "padding": "9px 10px",
                                                    "borderRadius": "10px",
                                                    "border": "1px solid rgba(26,35,126,0.14)",
                                                    "marginTop": "4px",
                                                },
                                            ),
                                        ]
                                    ),
                                    html.Div(
                                        style={"display": "flex", "alignItems": "flex-end"},
                                        children=html.Div(
                                            "Filters apply to all charts",
                                            style={"fontSize": "0.82rem", "color": "rgba(26,35,126,0.78)", "fontWeight": "900"},
                                        ),
                                    ),
                                ],
                            ),
                        ],
                    ),
 
                    # KPI TILES
                    html.Div(
                        className="kpi-grid",
                        children=[
                            _tile("Total Sessions", "tile-sessions", f"linear-gradient(90deg,{NAVY},{PRIMARY})"),
                            _tile("Unique Users", "tile-users", f"linear-gradient(90deg,{BLUE_1},{BLUE_2})"),
                            _tile("Total Usage (hrs)", "tile-avg-duration", f"linear-gradient(90deg,{SLATE_1},{SLATE_2})"),
                            _tile("Distinct Pages", "tile-pages", f"linear-gradient(90deg,{INDIGO_1},{INDIGO_2})"),
                            _tile("Distinct Modules", "tile-modules", f"linear-gradient(90deg,{SLATE_3},{SLATE_1})"),
                        ],
                    ),
 
                    # CHARTS
                    html.Div(
                        className="charts-grid-3",
                        children=[
                            _card("User Activity Over Time", "user-activity-chart", 360, "title-user-activity"),
                            _card("Most Used Modules (Visits)", "most-used-modules-chart", 400, "title-most-used-modules"),
                            _card("Most Active Time of Day", "active-time-chart", 360, "title-active-time"),
                        ],
                    ),
                    html.Div(
                        className="charts-grid-2",
                        children=[
                            _card("Module Flow (Hits by Module)", "module-flow-chart", 340, "title-module-flow"),
                            _card("Module Usage (Hours) in Selected Period", "module-hours-chart", 400, "title-module-hours"),
                        ],
                    ),
                    html.Div(
                        className="charts-grid-3",
                        children=[
                            _card("Top Users (by Visits)", "top-users-chart", 420, "title-top-users"),
                            _card("Total Usage per User (hours)", "avg-duration-chart", 420, "title-avg-duration"),
                            _card("IP-wise Activity", "ip-activity-chart", 420, "title-ip-activity"),
                        ],
                    ),
                ],
            ),
 
            # --------------------------------------------------
            # MODAL (popup)
            # --------------------------------------------------
            html.Div(
                id="graph-modal",
                style={"display": "none"},
                children=[
                    html.Div(
                        id="graph-modal-shell",
                        children=[
                            # click outside closes
                            html.Div(id="graph-modal-backdrop", n_clicks=0),
                            html.Div(
                                id="graph-modal-box",
                                children=[
                                    html.Div(
                                        style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"},
                                        children=[
                                            html.Div(
                                                id="graph-modal-title",
                                                style={"fontWeight": "950", "fontSize": "1.05rem", "color": NAVY},
                                            ),
                                            html.Button("✕", id="graph-modal-close", n_clicks=0, className="btn-refresh"),
                                        ],
                                    ),
                                    dcc.Graph(
                                        id="graph-modal-figure",
                                        config={"displayModeBar": True, "responsive": True},
                                        style={"height": "72vh"},
                                    ),
                                ],
                            ),
                        ],
                    )
                ],
            ),
 
            # FOOTER
            html.Footer(
                className="dsx-footer",
                children=[html.Div("Usage Analytics Dashboard · DataSolveX", style={"opacity": 0.95})],
            ),
        ],
    )

 
    # --------------------------------------------------
    # CALLBACKS
    # --------------------------------------------------
    @dash_app.callback(
        Output("custom-date-container", "style"),
        Input("filter-time-range", "value"),
    )
    def toggle_custom_date_container(range_value):
        return {"display": "block", "marginTop": "10px"} if range_value == "custom" else {"display": "none"}
 
    @dash_app.callback(
        [
            Output("tile-sessions", "children"),
            Output("tile-users", "children"),
            Output("tile-avg-duration", "children"),
            Output("tile-pages", "children"),
            Output("tile-modules", "children"),
            Output("user-activity-chart", "figure"),
            Output("top-users-chart", "figure"),
            Output("most-used-modules-chart", "figure"),
            Output("module-flow-chart", "figure"),
            Output("ip-activity-chart", "figure"),
            Output("avg-duration-chart", "figure"),
            Output("active-time-chart", "figure"),
            Output("module-hours-chart", "figure"),
        ],
        [
            Input("filter-time-range", "value"),
            Input("filter-custom-range", "start_date"),
            Input("filter-custom-range", "end_date"),
            Input("filter-module", "value"),
            Input("filter-login", "value"),
            Input("filter-ip", "value"),
            Input("btn-refresh", "n_clicks"),
        ],
    )
    def update_dashboard(time_range, custom_start, custom_end, module_name, login_contains, ip_contains, _refresh):
        # Time window
        if time_range == "custom":
            if not custom_start or not custom_end:
                e = empty_fig(340)
                return ("0", "0", "0.0", "0", "0", e, e, e, e, e, e, e, e)
 
            start_d = date.fromisoformat(custom_start)
            end_d = date.fromisoformat(custom_end)
            start_dt_obj = datetime.combine(start_d, datetime.min.time())
            end_dt_obj = datetime.combine(end_d, datetime.max.time())
        else:
            now = datetime.now()
            if time_range == "1h":
                start_dt_obj = now - timedelta(hours=1)
            elif time_range == "12h":
                start_dt_obj = now - timedelta(hours=12)
            elif time_range == "1d":
                start_dt_obj = now - timedelta(days=1)
            elif time_range == "1w":
                start_dt_obj = now - timedelta(days=7)
            elif time_range == "1m":
                start_dt_obj = now - timedelta(days=30)
            else:
                start_dt_obj = now - timedelta(days=30)
            end_dt_obj = now
 
        if start_dt_obj > end_dt_obj:
            e = empty_fig(340)
            return ("0", "0", "0.0", "0", "0", e, e, e, e, e, e, e, e)
 
        df = load_filtered_data(start_dt_obj, end_dt_obj, module_name, login_contains, ip_contains)
        if df.empty:
            e = empty_fig(340)
            return ("0", "0", "0.0", "0", "0", e, e, e, e, e, e, e, e)
 
        total_sessions = len(df)
        unique_users = df["login_name"].nunique()
        total_duration_sec = df["duration_seconds"].sum()
        total_usage_hours = round(total_duration_sec / 3600.0, 1) if pd.notna(total_duration_sec) else 0.0
        unique_pages = df["page"].nunique()
        unique_modules = df["module_name"].nunique()
 
        # User Activity Over Time
        daily = df.groupby("log_date").size().reset_index(name="sessions").sort_values("log_date")
        fig_daily = px.line(daily, x="log_date", y="sessions", markers=True, color_discrete_sequence=[BLUE_1])
        fig_daily.update_traces(line=dict(width=3), marker=dict(size=7))
        fig_daily.update_layout(xaxis_title="Date", yaxis_title="Sessions")
        fig_daily = apply_theme(fig_daily, height=360, legend="none", colorway=[BLUE_1])
 
        # Top Users (Visits)
        users = (
            df.groupby("login_name")
            .agg(visit_count=("login_name", "size"), total_seconds=("duration_seconds", "sum"))
            .reset_index()
        )
        users["total_minutes"] = users["total_seconds"] / 60.0
        users = users.sort_values("visit_count", ascending=False).head(10)
 
        fig_users = px.bar(
            users,
            x="visit_count",
            y="login_name",
            orientation="h",
            text="visit_count",
            hover_data={"total_minutes": ":.1f"},
            color_discrete_sequence=[SLATE_1],
        )
        fig_users.update_layout(xaxis_title="Visits", yaxis_title="User", bargap=0.16)
        fig_users.update_traces(cliponaxis=False)
        fig_users.update_yaxes(autorange="reversed")
        fig_users = apply_theme(fig_users, height=420, legend="none", colorway=[SLATE_1])
 
        # Most Used Modules (stacked): top modules + TOP 5 USERS ONLY + SMART Y-AXIS
        TOP_MODULES = 6
        TOP_USERS = 5
 
        #  Top modules by total visits
        mod_totals = (
            df.groupby("module_name")
            .size()
            .reset_index(name="total_visits")
            .sort_values("total_visits", ascending=False)
            .head(TOP_MODULES)
        )
 
        top_modules = mod_totals["module_name"].tolist()
 
        # Visits per (module, user)
        mod_user = (
            df[df["module_name"].isin(top_modules)]
            .groupby(["module_name", "login_name"])
            .size()
            .reset_index(name="visits")
        )
 
        # Global TOP 5 users only
        top_users = (
            mod_user.groupby("login_name")["visits"]
                    .sum()
                    .sort_values(ascending=False)
                    .head(TOP_USERS)
                    .index
                    .tolist()
        )
 
        mod_user_top = mod_user[mod_user["login_name"].isin(top_users)]
 
        # Order stacks consistently (largest contributor at bottom)
        mod_user_top = mod_user_top.sort_values(
            ["module_name", "visits"], ascending=[True, False]
        )
 
        # Total per module (for clean labels)
        module_totals = (
            mod_user_top.groupby("module_name")["visits"]
                        .sum()
                        .reindex(top_modules)
        )
 
        fig_most_modules = px.bar(
            mod_user_top,
            x="module_name",
            y="visits",
            color="login_name",
            barmode="stack",
            color_discrete_sequence=CAT_SERIES,
        )
 
        fig_most_modules.update_traces(
            texttemplate="%{y}",
            textposition="auto",
            insidetextanchor="middle",
            cliponaxis=False,
            textfont=dict(size=10)   # reduce if still crowded
        )
 
        # ----------------------------
        # VISUAL TUNING (THIS IS THE FIX)
        # ----------------------------
        fig_most_modules.update_layout(
            xaxis_title="Module",
            yaxis_title="Visits",
            xaxis_tickangle=-15,
            bargap=0.18,          # more breathing room
            bargroupgap=0.06,
            margin=dict(l=10, r=10, t=40, b=70),
        )
 
        # ❌ Kill per-segment labels (this is what ruins it)
        fig_most_modules.update_traces(
            text=mod_user_top["visits"],
            textposition="inside",
            insidetextanchor="middle",
            cliponaxis=False
        )
 
        # ✅ add totals OUTSIDE (top of stack) using scatter text
        fig_most_modules.add_trace(
            go.Scatter(
                x=top_modules,
                y=module_totals.values,
                text=[int(v) for v in module_totals.values],
                mode="text",
                textposition="top center",
                showlegend=False,
                hoverinfo="skip",
            )
)
 
        # ✅ Smart Y-axis headroom
        max_y = float(module_totals.max()) if not module_totals.empty else 0.0
        fig_most_modules.update_yaxes(range=[0, max_y * 1.25 if max_y else 1])
 
        # Stable module order
        fig_most_modules.update_xaxes(categoryorder="array", categoryarray=top_modules)
 
        fig_most_modules = apply_theme(
            fig_most_modules,
            height=400,
            legend="v",
            colorway=CAT_SERIES
        )
 
        # Module Flow (donut)
        modules_flow = (
            df.groupby("module_name").size().reset_index(name="visit_count").sort_values("visit_count", ascending=False)
        )
        fig_module_flow = px.pie(
            modules_flow,
            names="module_name",
            values="visit_count",
            hole=0.5,
            color_discrete_sequence=[PRIMARY, SLATE_1, BLUE_2, INDIGO_1, SLATE_2, INDIGO_2, BLUE_1, SLATE_3],
        )
        fig_module_flow = apply_theme(
            fig_module_flow,
            height=340,
            legend="h",
            colorway=[PRIMARY, SLATE_1, BLUE_2, INDIGO_1, SLATE_2, INDIGO_2, BLUE_1, SLATE_3],
        )
 
        # IP-wise Activity
        ip_agg = (
            df.groupby("client_ip")
            .size()
            .reset_index(name="visit_count")
            .sort_values("visit_count", ascending=False)
            .head(10)
        )
        fig_ip = px.bar(
            ip_agg,
            x="visit_count",
            y="client_ip",
            orientation="h",
            text="visit_count",
            color_discrete_sequence=[INDIGO_1],
        )
        fig_ip.update_layout(xaxis_title="Visits", yaxis_title="IP address", bargap=0.16)
        fig_ip.update_traces(cliponaxis=False)
        fig_ip.update_yaxes(autorange="reversed")
        fig_ip = apply_theme(fig_ip, height=420, legend="none", colorway=[INDIGO_1])
 
        # Total Usage per User (hours)
        total_user = df.groupby("login_name").agg(total_seconds=("duration_seconds", "sum")).reset_index()
        total_user["total_hours"] = (total_user["total_seconds"] / 3600.0).round(1)
        total_user = total_user.sort_values("total_hours", ascending=False).head(15)
 
        fig_avg = px.bar(
            total_user,
            x="total_hours",
            y="login_name",
            orientation="h",
            text="total_hours",
            color_discrete_sequence=[PRIMARY],
        )
        fig_avg.update_layout(xaxis_title="Total usage (hrs)", yaxis_title="User", bargap=0.16)
        fig_avg.update_traces(texttemplate="%{text:.1f}", textposition="outside", cliponaxis=False)
        fig_avg.update_yaxes(autorange="reversed")
        fig_avg = apply_theme(fig_avg, height=420, legend="none", colorway=[PRIMARY])
 
        # Most Active Time of Day
        df["enter_time"] = pd.to_datetime(df["enter_time"])
        hourly = df.copy()
        hourly["hour_of_day"] = hourly["enter_time"].dt.hour
        hourly_agg = hourly.groupby("hour_of_day").size().reset_index(name="hits").sort_values("hour_of_day")
 
        fig_active_time = px.bar(
            hourly_agg,
            x="hour_of_day",
            y="hits",
            text="hits",
            color_discrete_sequence=[BLUE_2],
        )
        fig_active_time.update_layout(xaxis_title="Hour of Day (0–23)", yaxis_title="Sessions", bargap=0.18)
        fig_active_time.update_xaxes(dtick=1)
        fig_active_time.update_traces(textposition="outside", cliponaxis=False)
        fig_active_time = apply_theme(fig_active_time, height=360, legend="none", colorway=[BLUE_2])
 
        # Module Usage Hours
        module_hours = df.groupby("module_name")["duration_seconds"].sum().reset_index(name="total_seconds")
        module_hours["total_hours"] = module_hours["total_seconds"] / 3600.0
        module_hours = module_hours.sort_values("total_hours", ascending=False).head(10)
 
        fig_module_hours = px.bar(
            module_hours,
            x="module_name",
            y="total_hours",
            text=module_hours["total_hours"].round(1),
            color_discrete_sequence=[SLATE_2],
        )
        fig_module_hours.update_layout(
            xaxis_title="Module",
            yaxis_title="Usage (hours)",
            xaxis_tickangle=-20,
            bargap=0.16,
        )
        fig_module_hours.update_traces(textposition="outside", cliponaxis=False)
        fig_module_hours.update_xaxes(categoryorder="array", categoryarray=module_hours["module_name"].tolist())
        fig_module_hours = apply_theme(fig_module_hours, height=400, legend="none", colorway=[SLATE_2])
 
        return (
            f"{total_sessions:,}",
            str(unique_users),
            f"{total_usage_hours:.1f}",
            str(unique_pages),
            str(unique_modules),
            fig_daily,
            fig_users,
            fig_most_modules,
            fig_module_flow,
            fig_ip,
            fig_avg,
            fig_active_time,
            fig_module_hours,
        )
 
    # --------------------------------------------------
    # MODAL OPEN/CLOSE + FIGURE POPULATION
    # --------------------------------------------------
    @dash_app.callback(
        [
            Output("graph-modal", "style"),
            Output("graph-modal-title", "children"),
            Output("graph-modal-figure", "figure"),
            Output("modal-chart-key", "data"),
        ],
        [
            Input("title-user-activity", "n_clicks"),
            Input("title-user-activity-fs", "n_clicks"),
            Input("title-most-used-modules", "n_clicks"),
            Input("title-most-used-modules-fs", "n_clicks"),
            Input("title-active-time", "n_clicks"),
            Input("title-active-time-fs", "n_clicks"),
            Input("title-module-flow", "n_clicks"),
            Input("title-module-flow-fs", "n_clicks"),
            Input("title-module-hours", "n_clicks"),
            Input("title-module-hours-fs", "n_clicks"),
            Input("title-top-users", "n_clicks"),
            Input("title-top-users-fs", "n_clicks"),
            Input("title-avg-duration", "n_clicks"),
            Input("title-avg-duration-fs", "n_clicks"),
            Input("title-ip-activity", "n_clicks"),
            Input("title-ip-activity-fs", "n_clicks"),
            Input("graph-modal-close", "n_clicks"),
            Input("graph-modal-backdrop", "n_clicks"),
        ],
        [
            State("user-activity-chart", "figure"),
            State("most-used-modules-chart", "figure"),
            State("active-time-chart", "figure"),
            State("module-flow-chart", "figure"),
            State("module-hours-chart", "figure"),
            State("top-users-chart", "figure"),
            State("avg-duration-chart", "figure"),
            State("ip-activity-chart", "figure"),
        ],
        prevent_initial_call=True,
    )
    def handle_modal(
        c1, c1fs,
        c2, c2fs,
        c3, c3fs,
        c4, c4fs,
        c5, c5fs,
        c6, c6fs,
        c7, c7fs,
        c8, c8fs,
        c_close, c_backdrop,
        fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8,
    ):
        from dash import callback_context
 
        if not callback_context.triggered:
            raise PreventUpdate
 
        trig = callback_context.triggered[0]["prop_id"].split(".")[0]
 
        # Close conditions
        if trig in ("graph-modal-close", "graph-modal-backdrop"):
            return {"display": "none"}, "", {}, ""
 
        key = trig.replace("-fs", "")
 
        mapping = {
            "title-user-activity": ("User Activity Over Time", fig1),
            "title-most-used-modules": ("Most Used Modules (Visits)", fig2),
            "title-active-time": ("Most Active Time of Day", fig3),
            "title-module-flow": ("Module Flow (Hits by Module)", fig4),
            "title-module-hours": ("Module Usage (Hours) in Selected Period", fig5),
            "title-top-users": ("Top Users (by Visits)", fig6),
            "title-avg-duration": ("Total Usage per User (hours)", fig7),
            "title-ip-activity": ("IP-wise Activity", fig8),
        }
 
        title, fig = mapping.get(key, ("", {}))
        if not fig:
            fig = empty_fig(520)
 
        # Modal sizing (bigger)
        try:
            fig = dict(fig)
            fig.setdefault("layout", {})
            fig["layout"]["height"] = 820
            fig["layout"]["margin"] = dict(l=30, r=30, t=70, b=30)
        except Exception:
            pass
 
        return {"display": "block"}, title, fig, key
 
    return dash_app
 
 