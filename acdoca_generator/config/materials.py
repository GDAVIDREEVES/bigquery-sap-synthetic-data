"""Per-industry synthetic material master for financial supply chain flows."""

from __future__ import annotations

from dataclasses import dataclass

from acdoca_generator.config.industries import canonical_industry_key


@dataclass(frozen=True)
class Material:
    matnr: str
    description: str
    material_type: str  # RAW, SEMI, FG
    standard_cost: float
    unit: str  # EA, KG, L


PHARMACEUTICAL_MATERIALS: tuple[Material, ...] = (
    Material("API-001", "Active Pharmaceutical Ingredient Alpha", "RAW", 150.0, "KG"),
    Material("DPR-001", "Drug Product Alpha 100mg", "SEMI", 320.0, "EA"),
    Material("FG-001", "Packaged Alpha 100mg x30", "FG", 450.0, "EA"),
)

MEDICAL_DEVICE_MATERIALS: tuple[Material, ...] = (
    Material("CMP-100", "Implant Component Alloy", "RAW", 280.0, "EA"),
    Material("ASM-200", "Sterile Assembly Kit", "SEMI", 890.0, "EA"),
    Material("FG-DEV-01", "Finished Device FG", "FG", 1200.0, "EA"),
)

CONSUMER_GOODS_MATERIALS: tuple[Material, ...] = (
    Material("RM-BEV-01", "Beverage Concentrate", "RAW", 4.5, "L"),
    Material("PKG-500", "Bottle Pack 500ml", "SEMI", 0.85, "EA"),
    Material("FG-BEV-500", "Retail Beverage 500ml", "FG", 1.2, "EA"),
)

TECHNOLOGY_MATERIALS: tuple[Material, ...] = (
    Material("HW-CHIP-01", "SoC Module", "RAW", 42.0, "EA"),
    Material("ASM-MB-02", "Motherboard Assembly", "SEMI", 180.0, "EA"),
    Material("FG-LAP-01", "Finished Laptop SKU", "FG", 650.0, "EA"),
)

MEDIA_MATERIALS: tuple[Material, ...] = (
    Material("LIC-CORE", "Core Content License", "RAW", 0.0, "EA"),
    Material("DIST-PKG", "Distribution Package", "SEMI", 2.5, "EA"),
    Material("FG-SUB", "Consumer Subscription Unit", "FG", 12.0, "EA"),
)

INDUSTRY_MATERIALS: dict[str, tuple[Material, ...]] = {
    "pharmaceutical": PHARMACEUTICAL_MATERIALS,
    "medical_device": MEDICAL_DEVICE_MATERIALS,
    "consumer_goods": CONSUMER_GOODS_MATERIALS,
    "technology": TECHNOLOGY_MATERIALS,
    "media": MEDIA_MATERIALS,
}


def materials_for_industry(industry_key: str) -> tuple[Material, ...]:
    ck = canonical_industry_key(industry_key)
    return INDUSTRY_MATERIALS.get(ck, PHARMACEUTICAL_MATERIALS)
