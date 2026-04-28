"""Multi-hop financial supply chain blueprints per industry.

Templates support both **goods** flows (default) and non-goods IC flow types
(royalty / IP licensing, service charges, cost-share recharges) via the
`transaction_type` discriminator on `SupplyChainStep`. Backward compat:
default `transaction_type="goods"` preserves every legacy step literal.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from acdoca_generator.config.industries import canonical_industry_key


@dataclass(frozen=True)
class SupplyChainStep:
    step_number: int
    source_role: str
    dest_role: str
    material_type: str
    material_index: int
    usage_factor: float
    tp_method_key: tuple[str, str]
    fanout_all: bool = False  # True = sell to every eligible buyer in the pool

    # Non-goods discriminator. Default keeps existing literals as goods flows.
    transaction_type: str = "goods"

    # Only consulted when transaction_type != "goods". When None, the rate/share
    # is drawn from the TP-method markup band for (source, dest, transaction_type).
    royalty_rate: Optional[float] = None        # used when transaction_type == "royalty"
    cost_pool_share: Optional[float] = None     # used when transaction_type == "cost_share"
    service_basis: Optional[str] = None         # informational; one of cost_plus_headcount / cost_plus_revenue_share / fixed_fee

    # Controversy / APA metadata. APA-covered steps use the tighter `apa_markup_band`
    # (typically lower than the role-pair default) instead of the TP-method-resolved band.
    apa_flag: bool = False
    apa_markup_band: Optional[tuple[float, float]] = None


# One or more template chains per industry (list of steps each).
# Each industry has at least 2 templates; chains[chain_i % len(chains)] cycles through
# them in supply_chain.generate_supply_chain_flows so n_chains >= 2 always exercises diversity.

PHARMA_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    # Classic pharma principal: toll mfg → IP principal → final formulation → distributor
    (
        SupplyChainStep(1, "TOLL", "IPPR", "RAW", 0, 1.2, ("TOLL", "IPPR")),
        SupplyChainStep(2, "IPPR", "FRMF", "SEMI", 1, 1.0, ("IPPR", "FRMF"), fanout_all=True),
        SupplyChainStep(3, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD")),
    ),
    # R&D services chain via commissionaire
    (
        SupplyChainStep(1, "RDSC", "IPPR", "RAW", 0, 1.0, ("RDSC", "IPPR")),
        SupplyChainStep(2, "IPPR", "COMM", "SEMI", 1, 1.0, ("IPPR", "COMM")),
        SupplyChainStep(3, "COMM", "LRD", "FG", 2, 1.0, ("COMM", "LRD")),
    ),
    # Royalty chain: IPPR licenses IP to FRMF, FRMF sublicenses (or pays distributors a royalty)
    (
        SupplyChainStep(
            1, "IPPR", "FRMF", "ROYALTY", 1, 1.0, ("IPPR", "FRMF"),
            fanout_all=True, transaction_type="royalty",
        ),
        SupplyChainStep(
            2, "IPPR", "LRD", "ROYALTY", 2, 1.0, ("IPPR", "LRD"),
            fanout_all=True, transaction_type="royalty",
        ),
    ),
    # Service chain: regional HQ recharges management fees to LRDs and FRMFs
    (
        SupplyChainStep(
            1, "RHQ", "LRD", "SERVICE", 0, 1.0, ("RHQ", "LRD"),
            fanout_all=True, transaction_type="service", service_basis="cost_plus_headcount",
        ),
        SupplyChainStep(
            2, "RHQ", "FRMF", "SERVICE", 0, 1.0, ("RHQ", "FRMF"),
            fanout_all=True, transaction_type="service", service_basis="cost_plus_headcount",
        ),
    ),
    # Cost-share chain: R&D pool recharged at-cost to IPPR (development cost contribution)
    (
        SupplyChainStep(
            1, "RDSC", "IPPR", "COST_SHARE", 0, 1.0, ("RDSC", "IPPR"),
            transaction_type="cost_share", cost_pool_share=0.6,
        ),
    ),
    # APA-covered chain: pre-agreed pricing with a tighter markup band
    # (mimics an Advance Pricing Agreement between the principal and tax authority).
    (
        SupplyChainStep(
            1, "TOLL", "IPPR", "RAW", 0, 1.2, ("TOLL", "IPPR"),
            apa_flag=True, apa_markup_band=(0.035, 0.045),
        ),
        SupplyChainStep(
            2, "IPPR", "FRMF", "SEMI", 1, 1.0, ("IPPR", "FRMF"),
            apa_flag=True, apa_markup_band=(-0.005, 0.005),
        ),
        SupplyChainStep(
            3, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD"),
            apa_flag=True, apa_markup_band=(-0.025, -0.015),
        ),
    ),
)

MED_DEVICE_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    # Contract mfg → IP principal → final → distributor
    (
        SupplyChainStep(1, "CMFR", "IPPR", "RAW", 0, 1.0, ("CMFR", "IPPR")),
        SupplyChainStep(2, "IPPR", "FRMF", "SEMI", 1, 1.0, ("IPPR", "FRMF"), fanout_all=True),
        SupplyChainStep(3, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD")),
    ),
    # Toll mfg → IP principal → distributor (shorter, no separate final-mfg step)
    (
        SupplyChainStep(1, "TOLL", "IPPR", "RAW", 0, 1.1, ("TOLL", "IPPR")),
        SupplyChainStep(2, "IPPR", "LRD", "FG", 2, 1.0, ("IPPR", "LRD")),
    ),
)

CPG_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    # Contract mfg → IP principal → final → distributor (replaces the unrealistic
    # LRD-as-seller variant that lived here before)
    (
        SupplyChainStep(1, "CMFR", "IPPR", "RAW", 0, 1.0, ("CMFR", "IPPR")),
        SupplyChainStep(2, "IPPR", "FRMF", "SEMI", 1, 1.0, ("IPPR", "FRMF"), fanout_all=True),
        SupplyChainStep(3, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD")),
    ),
    # Toll mfg straight to LRD (no IP principal in path, e.g. private-label CPG)
    (
        SupplyChainStep(1, "TOLL", "FRMF", "RAW", 0, 1.0, ("TOLL", "FRMF")),
        SupplyChainStep(2, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD")),
    ),
    # Brand royalty chain: IPPR licenses brand to LRDs in each market
    (
        SupplyChainStep(
            1, "IPPR", "LRD", "ROYALTY", 2, 1.0, ("IPPR", "LRD"),
            fanout_all=True, transaction_type="royalty",
        ),
    ),
)

TECH_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    # R&D services → IP principal → final → distributor
    (
        SupplyChainStep(1, "RDSC", "IPPR", "RAW", 0, 1.0, ("RDSC", "IPPR")),
        SupplyChainStep(2, "IPPR", "FRMF", "SEMI", 1, 1.0, ("IPPR", "FRMF"), fanout_all=True),
        SupplyChainStep(3, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD")),
    ),
    # Contract mfg → IP principal → commissionaire → distributor
    (
        SupplyChainStep(1, "CMFR", "IPPR", "RAW", 0, 1.0, ("CMFR", "IPPR")),
        SupplyChainStep(2, "IPPR", "COMM", "SEMI", 1, 1.0, ("IPPR", "COMM")),
        SupplyChainStep(3, "COMM", "LRD", "FG", 2, 1.0, ("COMM", "LRD")),
    ),
    # SaaS royalty chain: IPPR licenses platform to FRMF (regional principals)
    (
        SupplyChainStep(
            1, "IPPR", "FRMF", "ROYALTY", 1, 1.0, ("IPPR", "FRMF"),
            fanout_all=True, transaction_type="royalty",
        ),
    ),
    # Cost-share chain: R&D pool funded by IPPR, services recharged at-cost
    (
        SupplyChainStep(
            1, "RDSC", "IPPR", "COST_SHARE", 0, 1.0, ("RDSC", "IPPR"),
            transaction_type="cost_share", cost_pool_share=0.7,
        ),
    ),
)

MEDIA_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    # IP principal → regional HQ → distributor (existing pattern; keep RHQ → LRD profit split)
    (
        SupplyChainStep(1, "IPPR", "RHQ", "RAW", 0, 1.0, ("IPPR", "RHQ")),
        SupplyChainStep(2, "RHQ", "LRD", "SEMI", 1, 1.0, ("RHQ", "LRD"), fanout_all=True),
        SupplyChainStep(3, "RHQ", "LRD", "FG", 2, 1.0, ("RHQ", "LRD")),
    ),
    # R&D services → IP principal → distributor (content-production model)
    (
        SupplyChainStep(1, "RDSC", "IPPR", "RAW", 0, 1.0, ("RDSC", "IPPR")),
        SupplyChainStep(2, "IPPR", "LRD", "FG", 2, 1.0, ("IPPR", "LRD")),
    ),
)

INDUSTRY_SUPPLY_CHAINS: dict[str, tuple[tuple[SupplyChainStep, ...], ...]] = {
    "pharmaceutical": PHARMA_CHAINS,
    "medical_device": MED_DEVICE_CHAINS,
    "consumer_goods": CPG_CHAINS,
    "technology": TECH_CHAINS,
    "media": MEDIA_CHAINS,
}


def supply_chain_templates_for_industry(industry_key: str) -> tuple[tuple[SupplyChainStep, ...], ...]:
    ck = canonical_industry_key(industry_key)
    return INDUSTRY_SUPPLY_CHAINS.get(ck, PHARMA_CHAINS)
