"""Dash layout for financial supply chain viewer."""

from __future__ import annotations

from typing import Any, List

import dash_cytoscape as cyto
from dash import dash_table, dcc, html

from acdoca_generator.dash_app.graph import build_cytoscape_elements, filter_flow_records


def _table_columns(records: List[dict[str, Any]]) -> list[dict[str, str]]:
    if not records:
        return [
            {"name": "CHAIN_ID", "id": "CHAIN_ID"},
            {"name": "STEP_NUMBER", "id": "STEP_NUMBER"},
            {"name": "MATNR", "id": "MATNR"},
            {"name": "SELLING_COMPANY", "id": "SELLING_COMPANY"},
            {"name": "BUYING_COMPANY", "id": "BUYING_COMPANY"},
            {"name": "TP_METHOD", "id": "TP_METHOD"},
            {"name": "TOTAL_LEGAL_PRICE", "id": "TOTAL_LEGAL_PRICE"},
        ]
    return [{"name": k, "id": k} for k in records[0].keys()]


def _stylesheet() -> list[dict[str, Any]]:
    return [
        {"selector": "node", "style": {"label": "data(label)", "text-wrap": "wrap", "font-size": "10px"}},
        {"selector": "edge", "style": {"curve-style": "bezier", "target-arrow-shape": "triangle", "label": "data(label)", "font-size": "8px", "text-rotation": "autorotate"}},
        {"selector": ".role_ippr", "style": {"background-color": "#2563eb"}},
        {"selector": ".role_frmf", "style": {"background-color": "#0d9488"}},
        {"selector": ".role_lrd", "style": {"background-color": "#16a34a"}},
        {"selector": ".role_toll", "style": {"background-color": "#d97706"}},
        {"selector": ".role_cmfr", "style": {"background-color": "#ea580c"}},
        {"selector": ".role_rdsc", "style": {"background-color": "#7c3aed"}},
        {"selector": ".role_ssc", "style": {"background-color": "#db2777"}},
        {"selector": ".role_rhq", "style": {"background-color": "#4f46e5"}},
        {"selector": ".role_comm", "style": {"background-color": "#0891b2"}},
        {"selector": ".role_finc", "style": {"background-color": "#64748b"}},
        {"selector": ".role_default", "style": {"background-color": "#525252"}},
        {"selector": ".mat_raw", "style": {"line-style": "dashed", "line-color": "#92400e"}},
        {"selector": ".mat_semi", "style": {"line-style": "dotted", "line-color": "#0369a1"}},
        {"selector": ".mat_fg", "style": {"line-style": "solid", "line-color": "#15803d"}},
    ]


def build_layout(records: List[dict[str, Any]]) -> html.Div:
    filtered = filter_flow_records(records)
    elements, _, _ = build_cytoscape_elements(filtered)
    chains = sorted({str(r.get("CHAIN_ID", "")) for r in records if r.get("CHAIN_ID")})
    chain_options = [{"label": "All chains", "value": "ALL"}] + [{"label": c, "value": c} for c in chains]
    popers = sorted({str(r.get("POPER", "")) for r in records if r.get("POPER")})
    poper_options = [{"label": "All periods", "value": "ALL"}] + [{"label": p, "value": p} for p in popers]
    roles = sorted({str(r.get("SELLER_ROLE")) for r in records if r.get("SELLER_ROLE")} | {str(r.get("BUYER_ROLE")) for r in records if r.get("BUYER_ROLE")})

    return html.Div(
        [
            dcc.Store(id="flow-store", data=records),
            dcc.Store(id="graph-meta", data={}),
            html.H2("Financial supply chain trace", style={"margin": "8px 16px"}),
            html.Div(
                [
                    html.Div(
                        [
                            html.H4("Filters"),
                            html.Label("Chain"),
                            dcc.Dropdown(id="filter-chain", options=chain_options, value="ALL", clearable=False),
                            html.Br(),
                            html.Label("Posting period (POPER)"),
                            dcc.Dropdown(id="filter-poper", options=poper_options, value="ALL", clearable=False),
                            html.Br(),
                            html.Label("Material types"),
                            dcc.Checklist(
                                id="filter-mat",
                                options=[{"label": " RAW", "value": "RAW"}, {"label": " SEMI", "value": "SEMI"}, {"label": " FG", "value": "FG"}],
                                value=["RAW", "SEMI", "FG"],
                                inline=False,
                            ),
                            html.Br(),
                            html.Label("Entity roles (any match on hop)"),
                            dcc.Dropdown(
                                id="filter-role",
                                options=[{"label": r, "value": r} for r in roles],
                                multi=True,
                                placeholder="All roles",
                            ),
                            html.Br(),
                            html.Label("Layout"),
                            dcc.Dropdown(
                                id="layout-name",
                                options=[
                                    {"label": "Breadthfirst (flow)", "value": "breadthfirst"},
                                    {"label": "Cose", "value": "cose"},
                                    {"label": "Circle", "value": "circle"},
                                ],
                                value="breadthfirst",
                                clearable=False,
                            ),
                        ],
                        style={"width": "260px", "padding": "12px", "borderRight": "1px solid #ccc", "overflowY": "auto"},
                    ),
                    html.Div(
                        [
                            cyto.Cytoscape(
                                id="cyto",
                                elements=elements,
                                style={"width": "100%", "height": "480px"},
                                layout={"name": "breadthfirst", "directed": True, "spacingFactor": 1.2},
                                stylesheet=_stylesheet(),
                                userPanningEnabled=True,
                                userZoomingEnabled=True,
                                boxSelectionEnabled=True,
                            ),
                        ],
                        style={"flex": "1", "minWidth": "400px"},
                    ),
                    html.Div(
                        [
                            html.H4("Selection"),
                            dcc.Markdown(id="inspector", children="_Click a node or edge._"),
                        ],
                        style={"width": "320px", "padding": "12px", "borderLeft": "1px solid #ccc", "overflowY": "auto"},
                    ),
                ],
                style={"display": "flex", "flexDirection": "row", "alignItems": "stretch", "minHeight": "500px"},
            ),
            html.H4("Flow rows", style={"margin": "16px 16px 8px"}),
            html.Div(
                [
                    dash_table.DataTable(
                        id="flow-table",
                        columns=_table_columns(records),
                        data=records,
                        page_size=15,
                        sort_action="native",
                        filter_action="native",
                        style_table={"overflowX": "auto"},
                        style_cell={"textAlign": "left", "minWidth": "80px", "maxWidth": "180px", "overflow": "hidden", "textOverflow": "ellipsis"},
                    )
                ],
                style={"margin": "0 16px 24px"},
            ),
        ],
        style={"fontFamily": "system-ui, sans-serif"},
    )
