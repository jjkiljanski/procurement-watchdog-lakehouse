from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.silver.notice_types.contract_notice_split_models import (  # noqa: E402
    ContractNoticeCoreRawV1,
)


def test_multi_part_fields_allowed_when_section_4_1_9_gt_1() -> None:
    model = ContractNoticeCoreRawV1(
        section_4_1_9="2",
        section_4_1_10="Ofertę można składać na wszystkie części",
        section_4_2_5="184430,40 PLN",
    )
    assert model.section_4_1_9 == "2"


def test_multi_part_fields_rejected_when_single_part() -> None:
    with pytest.raises(ValueError):
        ContractNoticeCoreRawV1(
            section_4_1_9="1",
            section_4_1_10="Ofertę można składać na wszystkie części",
        )


def test_multi_part_fields_rejected_when_section_4_1_9_missing() -> None:
    with pytest.raises(ValueError):
        ContractNoticeCoreRawV1(
            section_4_1_10="Ofertę można składać na wszystkie części",
        )


def test_single_part_without_multi_only_fields_is_valid() -> None:
    model = ContractNoticeCoreRawV1(section_2_1="Zamówienia publicznego")
    assert model.section_2_1 == "Zamówienia publicznego"


def test_section_4_1_9_present_with_1_is_rejected() -> None:
    with pytest.raises(ValueError):
        ContractNoticeCoreRawV1(section_4_1_9="1")
