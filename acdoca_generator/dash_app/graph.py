"""Build Cytoscape elements from supply chain flow records (list of dicts)."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, List, Optional, Set

# Node border / class by entity role
ROLE_CLASSES: dict[str, str] = {
    "IPPR": "role_ippr",
    "FRMF": "role_frmf",
    "LRD": "role_lrd",
    "TOLL": "role_toll",
    "CMFR": "role_cmfr",
    "RDSC": "role_rdsc",
    "SSC": "role_ssc",
    "RHQ": "role_rhq",
    "COMM": "role_comm",
    "FINC": "role_finc",
}

EDGE_MAT_CLASSES: dict[str, str] = {
    "RAW": "mat_raw",
    "SEMI": "mat_semi",
    "FG": "mat_fg",
}


def _node_id(rbukrs: str) -> str:
    return f"n_{rbukrs}"


def filter_flow_records(
    records: List[dict[str, Any]],
    *,
    chain_id: Optional[str] = None,
    material_types: Optional[Set[str]] = None,
    poper: Optional[str] = None,
    role_filter: Optional[Set[str]] = None,
) -> List[dict[str, Any]]:
    material_types = material_types or {"RAW", "SEMI", "FG"}
    role_filter = role_filter or set()
    filtered: List[dict[str, Any]] = []
    for r in records:
        if chain_id and chain_id != "ALL" and str(r.get("CHAIN_ID", "")) != chain_id:
            continue
        mt = str(r.get("MATERIAL_TYPE", ""))
        if mt and mt not in material_types:
            continue
        p = str(r.get("POPER", ""))
        if poper and poper != "ALL" and p != poper:
            continue
        sr = str(r.get("SELLER_ROLE", ""))
        br = str(r.get("BUYER_ROLE", ""))
        if role_filter and sr not in role_filter and br not in role_filter:
            continue
        filtered.append(r)
    return filtered


def build_cytoscape_elements(
    filtered: List[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """
    Returns (elements, node_data_by_id, edge_data_by_id) for inspector callbacks.
    """
    nodes: dict[str, dict[str, Any]] = {}
    node_agg: dict[str, dict[str, float]] = defaultdict(lambda: {"in_vol": 0.0, "out_vol": 0.0, "in_price": 0.0, "out_price": 0.0})

    edge_data_by_id: dict[str, dict[str, Any]] = {}

    for r in filtered:
        sell = str(r.get("SELLING_COMPANY", ""))
        buy = str(r.get("BUYING_COMPANY", ""))
        if not sell or not buy:
            continue
        sid = _node_id(sell)
        bid = _node_id(buy)
        ip = str(r.get("IP_OWNER", ""))
        rpo = str(r.get("RESIDUAL_PROFIT_OWNER", ""))

        if sid not in nodes:
            nodes[sid] = {
                "id": sid,
                "rbukrs": sell,
                "land1": str(r.get("SELLER_LAND1", "")),
                "role": str(r.get("SELLER_ROLE", "")),
                "is_ip": sell == ip,
                "is_rpo": sell == rpo,
            }
        if bid not in nodes:
            nodes[bid] = {
                "id": bid,
                "rbukrs": buy,
                "land1": str(r.get("BUYER_LAND1", "")),
                "role": str(r.get("BUYER_ROLE", "")),
                "is_ip": buy == ip,
                "is_rpo": buy == rpo,
            }

        try:
            vol = float(r.get("TOTAL_VOLUME") or 0)
            price = float(r.get("TOTAL_LEGAL_PRICE") or 0)
        except (TypeError, ValueError):
            vol, price = 0.0, 0.0
        node_agg[sid]["out_vol"] += vol
        node_agg[sid]["out_price"] += price
        node_agg[bid]["in_vol"] += vol
        node_agg[bid]["in_price"] += price

        eid = f"e_{r.get('AWREF', '')}_{sell}_{buy}"
        edge_data_by_id[eid] = dict(r)

    elements: list[dict[str, Any]] = []
    for nid, meta in nodes.items():
        role = meta.get("role", "") or "UNK"
        cls = ROLE_CLASSES.get(role, "role_default")
        label = f"{meta['rbukrs']}\n{meta['land1']}\n{role}"
        elements.append(
            {
                "data": {
                    "id": nid,
                    "label": label,
                    "rbukrs": meta["rbukrs"],
                },
                "classes": cls,
            }
        )

    for r in filtered:
        sell = str(r.get("SELLING_COMPANY", ""))
        buy = str(r.get("BUYING_COMPANY", ""))
        if not sell or not buy:
            continue
        sid = _node_id(sell)
        bid = _node_id(buy)
        eid = f"e_{r.get('AWREF', '')}_{sell}_{buy}"
        mt = str(r.get("MATERIAL_TYPE", "FG"))
        ecls = EDGE_MAT_CLASSES.get(mt, "mat_fg")
        tp = str(r.get("TP_METHOD", ""))
        step = r.get("STEP_NUMBER", "")
        label = f"#{step} {tp}\n{r.get('MATNR', '')}"
        elements.append(
            {
                "data": {
                    "id": eid,
                    "source": sid,
                    "target": bid,
                    "label": label,
                },
                "classes": ecls,
            }
        )

    node_inspector = {nid: {**nodes[nid], **node_agg.get(nid, {})} for nid in nodes}
    return elements, node_inspector, edge_data_by_id


def summarize_node(node_id: str, node_inspector: dict[str, dict[str, Any]]) -> str:
    m = node_inspector.get(node_id) or {}
    lines = [
        f"**Company** {m.get('rbukrs', '')}",
        f"Country {m.get('land1', '')}  |  Role {m.get('role', '')}",
        f"IP owner: {m.get('is_ip', False)}  |  Residual profit owner: {m.get('is_rpo', False)}",
        f"Inbound volume: {m.get('in_vol', 0):,.3f}  |  Outbound volume: {m.get('out_vol', 0):,.3f}",
        f"Inbound price: {m.get('in_price', 0):,.2f}  |  Outbound price: {m.get('out_price', 0):,.2f}",
    ]
    return "\n\n".join(lines)


def summarize_edge(edge_id: str, edge_data: dict[str, dict[str, Any]]) -> str:
    r = edge_data.get(edge_id) or {}
    if not r:
        return "No edge data."
    keys = [
        ("CHAIN_ID", "Chain"),
        ("STEP_NUMBER", "Step"),
        ("MATERIAL_TYPE", "Material type"),
        ("MATNR", "Material"),
        ("IP_OWNER", "IP owner"),
        ("SELLING_COMPANY", "Seller"),
        ("BUYING_COMPANY", "Buyer"),
        ("RESIDUAL_PROFIT_OWNER", "Residual profit owner"),
        ("WERKS", "Plant"),
        ("TP_METHOD", "TP method"),
        ("USAGE_FACTOR", "Usage factor"),
        ("TOTAL_VOLUME", "Total volume"),
        ("STANDARD_COST", "Standard cost"),
        ("MARKUP_RATE", "Markup / discount rate"),
        ("TOTAL_LEGAL_PRICE", "Total legal price"),
        ("GJAHR", "Fiscal year"),
        ("POPER", "Period"),
        ("AWREF", "AWREF"),
    ]
    parts = []
    for k, lab in keys:
        if k in r:
            parts.append(f"**{lab}**  \n{r.get(k)}")
    return "\n\n".join(parts)
