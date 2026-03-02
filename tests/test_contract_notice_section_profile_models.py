from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.silver.notice_types.contract_notice_section_profile_models import (
    ContractNoticeSectionProfile,
)


def test_contract_notice_single_part_profile_model_parses_generated_json() -> None:
    path = Path(
        "examples/contractnotice_sections/contractnotice_2025-10-01_single_part_sections_unique.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    model = ContractNoticeSectionProfile.model_validate(payload)

    assert model.noticeType == "ContractNotice"
    assert model.contract_class == "single_part"
    assert model.contract_count > 0
    assert len(model.sections) > 0

