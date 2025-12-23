# log_dash.py

import os
from datetime import date, datetime, timedelta

import pyodbc
import pandas as pd
import plotly.express as px

from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from dotenv import load_dotenv

# --------------------------------------------------
# DB CONFIG
# --------------------------------------------------
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))


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
# PAGE -> MODULE MAPPING
# --------------------------------------------------
PAGE_TO_MODULE = {
    "/dbpage.html": "Dbrefresh",
    "/dashboard/summary.html": "Replication_summary",
    "/dbrefresh.html": "Dbrefresh",
    "/dashboard.html": "Replication_details",
    "/login.html": "login",
    "/index.html": "Database Main",
    "/replication_dashboard/summary.html": "Replication_summary",
    "/replication_dashboard.html": "Replication_details",
    "/database_page.html": "Dbrefresh",
    "/sqlserver_main.html": "SQL_server",
    "/form.html": "Login Creation",
}


def map_page_to_module(page: str) -> str:
    return PAGE_TO_MODULE.get(page, page or "Unknown")


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
        modules_set.add(map_page_to_module(p))

    modules_list = sorted(modules_set)

    # Guard in case table is empty
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
    else:
        df["module_name"] = pd.Series(dtype=str)

    if module_name and module_name != "All modules":
        df = df[df["module_name"] == module_name]

    return df


# --------------------------------------------------
# SMALL UI HELPERS
# --------------------------------------------------
def _tile(label, id_):
    return html.Div(
        style={
            "backgroundColor": "#ffffff",
            "borderRadius": "10px",
            "padding": "10px 12px",
            "border": "1px solid #e5e7eb",
            "boxShadow": "0 4px 10px rgba(15,23,42,0.04)",
        },
        children=[
            html.Div(label, style={"fontSize": "0.8rem", "color": "#6b7280"}),
            html.Div(id=id_, style={"fontSize": "1.3rem", "fontWeight": "600"}),
        ],
    )


def _card(title, graph_id):
    return html.Div(
        style={
            "backgroundColor": "#ffffff",
            "borderRadius": "10px",
            "border": "1px solid #e5e7eb",
            "padding": "12px 16px",
            "boxShadow": "0 6px 14px rgba(15,23,42,0.05)",
        },
        children=[
            html.H2(title, style={"fontSize": "0.95rem", "marginBottom": "8px"}),
            dcc.Graph(id=graph_id, figure={}, config={"displayModeBar": False}),
        ],
    )


# --------------------------------------------------
# DASH APP FACTORY
# --------------------------------------------------
def create_log_dash(flask_app):
    modules_list, min_date, max_date = load_filter_options()

    dash_app = Dash(
        __name__,
        server=flask_app,
        url_base_pathname="/loganalytics/",
        suppress_callback_exceptions=True,
    )
    dash_app.title = "Usage Analytics Dashboard"

    dash_app.index_string = """
    <!DOCTYPE html>
    <html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link rel="stylesheet" href="/static/global.css">
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
    </html>
    """

    # ✅ Outer wrapper makes footer stick to bottom
    dash_app.layout = html.Div(
        style={
            "minHeight": "100vh",
            "display": "flex",
            "flexDirection": "column",
            "backgroundColor": "#f5f5f7",
            "fontFamily": "system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
        },
        children=[
            dcc.Location(id="dash-url", refresh=True),

            # ✅ Content wrapper (your original maxWidth layout)
            html.Div(
                style={
                    "maxWidth": "1200px",
                    "margin": "0 auto",
                    "width": "100%",
                    "flex": "1",
                    "paddingBottom": "24px",
                },
                children=[
                    # ==================================================
                    # DataSolveX HEADER + BLUE BREADCRUMB BAR
                    # ==================================================
                    html.Header(
                        className="app-header",
                        children=[
                            html.Nav(
                                className="app-nav",
                                children=html.Div(
                                    className="app-nav-container",
                                    children=[
                                        dcc.Link("DataSolveX", href="/", className="app-brand", refresh=True),
                                    ],
                                ),
                            ),

                            html.Div(
                                className="app-breadcrumb-bar",
                                style={"backgroundColor": "#023e8a", "color": "#ffffff"},
                                children=html.Div(
                                    className="app-breadcrumb-container",
                                    style={
                                        "display": "flex",
                                        "alignItems": "center",
                                        "justifyContent": "space-between",
                                        "gap": "12px",
                                        "width": "100%",
                                        "padding": "10px 24px",
                                    },
                                    children=[
                                        html.Div(
                                            className="app-breadcrumb",
                                            style={"color": "#ffffff"},
                                            children=[
                                                dcc.Link(
                                                    "Login",
                                                    href="/login",
                                                    refresh=True,
                                                    style={"color": "#ffffff", "textDecoration": "none", "fontWeight": "600"},
                                                ),
                                                html.Span("›", style={"margin": "0 10px", "opacity": 0.85, "color": "#ffffff"}),
                                                dcc.Link(
                                                    "Admin",
                                                    href="/admin-home",
                                                    refresh=True,
                                                    style={"color": "#ffffff", "textDecoration": "none", "fontWeight": "600"},
                                                ),
                                                html.Span("›", style={"margin": "0 10px", "opacity": 0.85, "color": "#ffffff"}),
                                                html.Span("Usage Analytics", style={"fontWeight": "800", "color": "#ffffff"}),
                                            ],
                                        ),

                                        # ✅ IMPORTANT: add btn-refresh because callback uses it
                                        html.Button(
                                            "Refresh",
                                            id="btn-refresh",
                                            n_clicks=0,
                                            style={
                                                "background": "transparent",
                                                "border": "1px solid rgba(255,255,255,0.45)",
                                                "color": "#ffffff",
                                                "padding": "6px 10px",
                                                "borderRadius": "8px",
                                                "cursor": "pointer",
                                                "fontWeight": "700",
                                            },
                                        ),
                                    ],
                                ),
                            ),
                        ],
                    ),

                    # ==================================================
                    # FILTERS
                    # ==================================================
                    html.Div(
                        style={
                            "backgroundColor": "#ffffff",
                            "borderBottom": "1px solid #e5e7eb",
                            "padding": "12px 24px 6px",
                        },
                        children=[
                            html.Div(
                                style={
                                    "display": "flex",
                                    "flexWrap": "wrap",
                                    "gap": "16px",
                                    "marginBottom": "8px",
                                },
                                children=[
                                    html.Div(
                                        style={"display": "flex", "flexDirection": "column", "minWidth": "200px"},
                                        children=[
                                            html.Label("Time range", style={"fontSize": "0.8rem", "color": "#6b7280"}),
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
                                        ],
                                    ),
                                    html.Div(
                                        id="custom-date-container",
                                        style={"display": "none", "flexDirection": "column", "minWidth": "260px"},
                                        children=[
                                            html.Label("Custom date range", style={"fontSize": "0.8rem", "color": "#6b7280"}),
                                            dcc.DatePickerRange(
                                                id="filter-custom-range",
                                                start_date=min_date,
                                                end_date=max_date,
                                                min_date_allowed=min_date,
                                                max_date_allowed=max_date,
                                            ),
                                        ],
                                    ),
                                    html.Div(
                                        style={"display": "flex", "flexDirection": "column", "minWidth": "200px"},
                                        children=[
                                            html.Label("Module", style={"fontSize": "0.8rem", "color": "#6b7280"}),
                                            dcc.Dropdown(
                                                id="filter-module",
                                                options=[{"label": "All modules", "value": "All modules"}]
                                                + [{"label": m, "value": m} for m in modules_list],
                                                value="All modules",
                                                clearable=False,
                                            ),
                                        ],
                                    ),
                                    html.Div(
                                        style={"display": "flex", "flexDirection": "column", "minWidth": "180px"},
                                        children=[
                                            html.Label("Login name contains", style={"fontSize": "0.8rem", "color": "#6b7280"}),
                                            dcc.Input(
                                                id="filter-login",
                                                type="text",
                                                placeholder="e.g. sakshi",
                                                style={"padding": "6px 10px", "borderRadius": "6px", "border": "1px solid #e5e7eb"},
                                            ),
                                        ],
                                    ),
                                    html.Div(
                                        style={"display": "flex", "flexDirection": "column", "minWidth": "160px"},
                                        children=[
                                            html.Label("IP contains", style={"fontSize": "0.8rem", "color": "#6b7280"}),
                                            dcc.Input(
                                                id="filter-ip",
                                                type="text",
                                                placeholder="e.g. 127.0.0.1",
                                                style={"padding": "6px 10px", "borderRadius": "6px", "border": "1px solid #e5e7eb"},
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                        ],
                    ),

                    # ==================================================
                    # SUMMARY TILES
                    # ==================================================
                    html.Div(
                        style={
                            "padding": "8px 24px 4px",
                            "display": "grid",
                            "gridTemplateColumns": "repeat(auto-fit, minmax(150px, 1fr))",
                            "gap": "12px",
                        },
                        children=[
                            _tile("Total Sessions", "tile-sessions"),
                            _tile("Unique Users", "tile-users"),
                            _tile("Avg Session Duration (min)", "tile-avg-duration"),
                            _tile("Distinct Pages", "tile-pages"),
                            _tile("Distinct Modules", "tile-modules"),
                        ],
                    ),

                    # ==================================================
                    # CHARTS
                    # ==================================================
                    html.Div(
                        style={"padding": "8px 24px 24px", "display": "flex", "flexDirection": "column", "gap": "16px"},
                        children=[
                            _card("User Activity Over Time", "user-activity-chart"),
                            _card("Top Users (by Visits)", "top-users-chart"),
                            _card("Most Used Modules (Visits)", "most-used-modules-chart"),
                            _card("Module Usage (Hours) in Selected Period", "module-hours-chart"),
                            _card("Module Flow (Hits by Module)", "module-flow-chart"),
                            _card("IP-wise Activity", "ip-activity-chart"),
                            _card("Avg Session Duration per User (seconds)", "avg-duration-chart"),
                            _card("Most Active Time of Day", "active-time-chart"),
                        ],
                    ),
                ],
            ),

            # ==================================================
            # FOOTER (fixed to bottom)
            # ==================================================
            html.Footer(
                style={
                    "marginTop": "auto",
                    "background": "linear-gradient(90deg, #03045e, #023e8a)",
                    "color": "#ffffff",
                    "padding": "14px 24px",
                    "borderTop": "1px solid rgba(255,255,255,0.12)",
                    "textAlign": "center",
                    "fontWeight": "700",
                    "letterSpacing": "0.2px",
                },
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
        base_style = {"flexDirection": "column", "minWidth": "260px"}
        return {"display": "flex", **base_style} if range_value == "custom" else {"display": "none", **base_style}

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
        def make_empty_fig():
            fig = px.line()
            fig.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30))
            fig.add_annotation(
                text="No data for selected filters",
                x=0.5,
                y=0.5,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=14, color="#6b7280"),
            )
            fig.update_xaxes(visible=False)
            fig.update_yaxes(visible=False)
            return fig

        empty_fig = make_empty_fig()

        if time_range == "custom":
            if not custom_start or not custom_end:
                return ("0", "0", "0.0", "0", "0", empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig)

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
            return ("0", "0", "0.0", "0", "0", empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig)

        df = load_filtered_data(start_dt_obj, end_dt_obj, module_name, login_contains, ip_contains)
        if df.empty:
            return ("0", "0", "0.0", "0", "0", empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig)

        total_sessions = len(df)
        unique_users = df["login_name"].nunique()

        avg_duration_sec = df["duration_seconds"].mean()
        avg_duration_min = round(avg_duration_sec / 60.0, 1) if pd.notna(avg_duration_sec) else 0.0

        unique_pages = df["page"].nunique()
        unique_modules = df["module_name"].nunique()

        daily = df.groupby("log_date").size().reset_index(name="sessions").sort_values("log_date")
        fig_daily = px.line(daily, x="log_date", y="sessions", markers=True)
        fig_daily.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30), xaxis_title="Date", yaxis_title="Sessions")

        users = (
            df.groupby("login_name")
            .agg(visit_count=("login_name", "size"), total_seconds=("duration_seconds", "sum"))
            .reset_index()
        )
        users["total_minutes"] = users["total_seconds"] / 60.0
        users = users.sort_values("visit_count", ascending=False).head(10)
        fig_users = px.bar(users, x="visit_count", y="login_name", orientation="h", text="visit_count", hover_data={"total_minutes": ":.1f"})
        fig_users.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30), xaxis_title="Visits", yaxis_title="User")

        modules_agg = df.groupby("module_name").size().reset_index(name="visit_count").sort_values("visit_count", ascending=False).head(10)
        fig_most_modules = px.bar(modules_agg, x="module_name", y="visit_count", text="visit_count")
        fig_most_modules.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30), xaxis_title="Module", yaxis_title="Visits", xaxis_tickangle=-20)

        modules_flow = df.groupby("module_name").size().reset_index(name="visit_count").sort_values("visit_count", ascending=False)
        fig_module_flow = px.pie(modules_flow, names="module_name", values="visit_count", hole=0.5)
        fig_module_flow.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30), legend_title_text="Module")

        ip_agg = df.groupby("client_ip").size().reset_index(name="visit_count").sort_values("visit_count", ascending=False).head(10)
        fig_ip = px.bar(ip_agg, x="visit_count", y="client_ip", orientation="h", text="visit_count")
        fig_ip.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30), xaxis_title="Visits", yaxis_title="IP address")

        avg_user = df.groupby("login_name")["duration_seconds"].mean().reset_index(name="avg_seconds").sort_values("avg_seconds", ascending=False).head(15)
        avg_user["avg_seconds"] = avg_user["avg_seconds"].round(0)
        fig_avg = px.bar(avg_user, x="avg_seconds", y="login_name", orientation="h", text="avg_seconds")
        fig_avg.update_traces(texttemplate="%{text:.0f}", textposition="outside")
        fig_avg.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30), xaxis_title="Avg duration (sec)", yaxis_title="User")

        df["enter_time"] = pd.to_datetime(df["enter_time"])
        hourly = df.copy()
        hourly["hour_of_day"] = hourly["enter_time"].dt.hour
        hourly_agg = hourly.groupby("hour_of_day").size().reset_index(name="hits").sort_values("hour_of_day")
        fig_active_time = px.bar(hourly_agg, x="hour_of_day", y="hits", text="hits")
        fig_active_time.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30), xaxis_title="Hour of Day (0–23)", yaxis_title="Sessions")
        fig_active_time.update_xaxes(dtick=1)
        fig_active_time.update_traces(textposition="outside")

        module_hours = df.groupby("module_name")["duration_seconds"].sum().reset_index(name="total_seconds")
        module_hours["total_hours"] = module_hours["total_seconds"] / 3600.0
        module_hours = module_hours.sort_values("total_hours", ascending=False).head(10)
        fig_module_hours = px.bar(module_hours, x="module_name", y="total_hours", text=module_hours["total_hours"].round(1))
        fig_module_hours.update_traces(textposition="outside")
        fig_module_hours.update_layout(template="plotly_white", height=260, margin=dict(l=30, r=20, t=30, b=30), xaxis_title="Module", yaxis_title="Usage (hours)", xaxis_tickangle=-20)

        return (
            f"{total_sessions:,}",
            str(unique_users),
            f"{avg_duration_min:.1f}",
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

    return dash_app
