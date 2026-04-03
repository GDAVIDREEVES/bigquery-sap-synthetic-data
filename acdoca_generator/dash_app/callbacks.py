"""Dash callbacks for supply chain viewer."""

from __future__ import annotations

import json
from typing import Any, List

from dash import Input, Output, State

from acdoca_generator.dash_app.graph import (
    build_cytoscape_elements,
    filter_flow_records,
    summarize_edge,
    summarize_node,
)


def _safe_meta(nodes: dict[str, Any], edges: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps({"nodes": nodes, "edges": edges}, default=str))


def register_callbacks(app: Any) -> None:
    @app.callback(
        Output("cyto", "elements"),
        Output("cyto", "layout"),
        Output("flow-table", "data"),
        Output("flow-table", "columns"),
        Output("graph-meta", "data"),
        Input("filter-chain", "value"),
        Input("filter-poper", "value"),
        Input("filter-mat", "value"),
        Input("filter-role", "value"),
        Input("layout-name", "value"),
        State("flow-store", "data"),
    )
    def update_graph(
        chain: str,
        poper: str,
        mat: List[str],
        roles: List[str],
        layout_name: str,
        records: List[dict[str, Any]],
    ):
        if not records:
            cols = [
                {"name": "CHAIN_ID", "id": "CHAIN_ID"},
                {"name": "STEP_NUMBER", "id": "STEP_NUMBER"},
                {"name": "MATNR", "id": "MATNR"},
            ]
            return [], {"name": "breadthfirst", "directed": True}, [], cols, {}
        mat_set = set(mat) if mat else {"RAW", "SEMI", "FG"}
        role_set = set(roles) if roles else set()
        filtered = filter_flow_records(
            records,
            chain_id=chain,
            material_types=mat_set,
            poper=poper,
            role_filter=role_set,
        )
        elements, node_map, edge_map = build_cytoscape_elements(filtered)
        layout = {"name": layout_name or "breadthfirst", "directed": True, "spacingFactor": 1.2}
        if not filtered:
            cols = [{"name": k, "id": k} for k in records[0].keys()]
            return elements, layout, [], cols, _safe_meta(node_map, edge_map)
        cols = [{"name": k, "id": k} for k in filtered[0].keys()]
        serial_rows = json.loads(json.dumps(filtered, default=str))
        return elements, layout, serial_rows, cols, _safe_meta(node_map, edge_map)

    @app.callback(
        Output("inspector", "children"),
        Input("cyto", "tapNodeData"),
        Input("cyto", "tapEdgeData"),
        State("graph-meta", "data"),
    )
    def show_inspector(node_data, edge_data, meta):
        meta = meta or {}
        nodes = meta.get("nodes") or {}
        edges = meta.get("edges") or {}
        if edge_data:
            eid = edge_data[0].get("id")
            return summarize_edge(str(eid), edges if isinstance(edges, dict) else {})
        if node_data:
            nid = node_data[0].get("id")
            return summarize_node(str(nid), {str(k): v for k, v in nodes.items()})
        return "_Click a node or edge._"
