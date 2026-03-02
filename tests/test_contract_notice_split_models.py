from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.silver.notice_types.contract_notice_split_models import (  # noqa: E402
    ContractNoticeCoreRawV1,
)


def test_multi_part_fields_allowed_when_cn_section_4_1_9_gt_1() -> None:
    model = ContractNoticeCoreRawV1(
        cn_section_4_1_9="2",
        cn_section_4_1_10="OfertÄ™ moĹĽna skĹ‚adaÄ‡ na wszystkie czÄ™Ĺ›ci",
        cn_section_4_2_5="184430,40 PLN",
    )
    assert model.cn_section_4_1_9 == "2"


def test_multi_part_fields_rejected_when_single_part() -> None:
    with pytest.raises(ValueError):
        ContractNoticeCoreRawV1(
            cn_section_4_1_9="1",
            cn_section_4_1_10="OfertÄ™ moĹĽna skĹ‚adaÄ‡ na wszystkie czÄ™Ĺ›ci",
        )


def test_multi_part_fields_rejected_when_cn_section_4_1_9_missing() -> None:
    with pytest.raises(ValueError):
        ContractNoticeCoreRawV1(
            cn_section_4_1_10="OfertÄ™ moĹĽna skĹ‚adaÄ‡ na wszystkie czÄ™Ĺ›ci",
        )


def test_single_part_without_multi_only_fields_is_valid() -> None:
    model = ContractNoticeCoreRawV1(cn_section_2_1="ZamĂłwienia publicznego")
    assert model.cn_section_2_1 == "ZamĂłwienia publicznego"


def test_cn_section_4_1_9_present_with_1_is_rejected() -> None:
    with pytest.raises(ValueError):
        ContractNoticeCoreRawV1(cn_section_4_1_9="1")



