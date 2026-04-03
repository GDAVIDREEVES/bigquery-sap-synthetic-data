"""Multi-hop financial supply chain blueprints per industry."""

from __future__ import annotations

from dataclasses import dataclass

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


# One or more template chains per industry (list of steps each).
PHARMA_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    (
        SupplyChainStep(1, "TOLL", "IPPR", "RAW", 0, 1.2, ("TOLL", "IPPR")),
        SupplyChainStep(2, "IPPR", "FRMF", "SEMI", 1, 1.0, ("IPPR", "FRMF")),
        SupplyChainStep(3, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD")),
    ),
)

MED_DEVICE_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    (
        SupplyChainStep(1, "CMFR", "IPPR", "RAW", 0, 1.0, ("CMFR", "IPPR")),
        SupplyChainStep(2, "IPPR", "FRMF", "SEMI", 1, 1.0, ("IPPR", "FRMF")),
        SupplyChainStep(3, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD")),
    ),
)

CPG_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    (
        SupplyChainStep(1, "LRD", "FRMF", "RAW", 0, 1.0, ("LRD", "FRMF")),
        SupplyChainStep(2, "FRMF", "LRD", "SEMI", 1, 1.0, ("FRMF", "LRD")),
        SupplyChainStep(3, "LRD", "LRD", "FG", 2, 1.0, ("LRD", "LRD")),
    ),
)

TECH_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    (
        SupplyChainStep(1, "RDSC", "IPPR", "RAW", 0, 1.0, ("RDSC", "IPPR")),
        SupplyChainStep(2, "IPPR", "FRMF", "SEMI", 1, 1.0, ("IPPR", "FRMF")),
        SupplyChainStep(3, "FRMF", "LRD", "FG", 2, 1.0, ("FRMF", "LRD")),
    ),
)

MEDIA_CHAINS: tuple[tuple[SupplyChainStep, ...], ...] = (
    (
        SupplyChainStep(1, "IPPR", "RHQ", "RAW", 0, 1.0, ("IPPR", "RHQ")),
        SupplyChainStep(2, "RHQ", "LRD", "SEMI", 1, 1.0, ("RHQ", "LRD")),
        SupplyChainStep(3, "LRD", "LRD", "FG", 2, 1.0, ("LRD", "LRD")),
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
