"""Tests for BZP bronze layer models and validation."""

import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest

# Allow imports from src/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from pydantic import ValidationError

from procurement.bronze.models import (
    NOTICE_TYPES,
    BzpNoticeBronze,
    BzpNoticeBronzeOut,
    ContractorDto,
    notice_record_hash,
    to_bronze_output,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def sample_records() -> list[dict]:
    return json.loads((FIXTURES / "sample_raw.json").read_text(encoding="utf-8"))


@pytest.fixture
def valid_record(sample_records: list[dict]) -> dict:
    return deepcopy(sample_records[0])


@pytest.fixture
def record_with_contractors(sample_records: list[dict]) -> dict:
    return deepcopy(sample_records[1])


# --- ContractorDto ---


class TestContractorDto:
    def test_all_fields_present(self):
        c = ContractorDto(
            contractorName="Acme",
            contractorCity="Warsaw",
            contractorProvince="PL12",
            contractorCountry="PL",
            contractorNationalId="1234567890",
        )
        assert c.contractorName == "Acme"

    def test_all_fields_none(self):
        c = ContractorDto()
        assert c.contractorName is None
        assert c.contractorCity is None


# --- BzpNoticeBronze validation ---


class TestBzpNoticeBronzeValidation:
    def test_valid_record_passes(self, valid_record: dict):
        notice = BzpNoticeBronze.model_validate(valid_record)
        assert notice.objectId == valid_record["objectId"]
        assert notice.noticeType == "ContractNotice"

    def test_record_with_contractors(self, record_with_contractors: dict):
        notice = BzpNoticeBronze.model_validate(record_with_contractors)
        assert notice.contractors is not None
        assert len(notice.contractors) >= 1
        assert notice.contractors[0].contractorName is not None

    def test_null_contractors_accepted(self, valid_record: dict):
        valid_record["contractors"] = None
        notice = BzpNoticeBronze.model_validate(valid_record)
        assert notice.contractors is None

    def test_truncated_html_rejected(self, valid_record: dict):
        valid_record["htmlBody"] = "<html><head></head><body>cut off here"
        with pytest.raises(ValidationError, match="truncated"):
            BzpNoticeBronze.model_validate(valid_record)

    def test_html_with_trailing_whitespace_accepted(self, valid_record: dict):
        valid_record["htmlBody"] = "<html><body>ok</body></html>  \n"
        notice = BzpNoticeBronze.model_validate(valid_record)
        assert notice.htmlBody.rstrip().endswith("</html>")

    def test_unknown_notice_type_rejected(self, valid_record: dict):
        valid_record["noticeType"] = "UnknownType"
        with pytest.raises(ValidationError, match="Unknown noticeType"):
            BzpNoticeBronze.model_validate(valid_record)

    def test_all_known_notice_types_accepted(self, valid_record: dict):
        for nt in NOTICE_TYPES:
            valid_record["noticeType"] = nt
            notice = BzpNoticeBronze.model_validate(valid_record)
            assert notice.noticeType == nt

    def test_missing_required_field_rejected(self, valid_record: dict):
        del valid_record["objectId"]
        with pytest.raises(ValidationError):
            BzpNoticeBronze.model_validate(valid_record)

    def test_nullable_fields_accept_none(self, valid_record: dict):
        valid_record["orderObject"] = None
        valid_record["clientType"] = None
        valid_record["orderType"] = None
        valid_record["organizationProvince"] = None
        valid_record["tenderId"] = None
        notice = BzpNoticeBronze.model_validate(valid_record)
        assert notice.orderObject is None
        assert notice.tenderId is None


# --- to_bronze_output ---


class TestToBronzeOutput:
    def test_html_replaced_with_sha256(self, valid_record: dict):
        notice = BzpNoticeBronze.model_validate(valid_record)
        out = to_bronze_output(notice)
        expected_hash = hashlib.sha256(
            notice.htmlBody.encode("utf-8")
        ).hexdigest()
        assert out.htmlBodySha256 == expected_hash
        assert not hasattr(out, "htmlBody") or "htmlBody" not in out.model_fields

    def test_output_is_correct_type(self, valid_record: dict):
        notice = BzpNoticeBronze.model_validate(valid_record)
        out = to_bronze_output(notice)
        assert isinstance(out, BzpNoticeBronzeOut)

    def test_all_fields_preserved(self, valid_record: dict):
        notice = BzpNoticeBronze.model_validate(valid_record)
        out = to_bronze_output(notice)
        for field in BzpNoticeBronzeOut.model_fields:
            if field == "htmlBodySha256":
                continue
            assert getattr(out, field) == getattr(notice, field), (
                f"Field {field} differs"
            )

    def test_hash_is_deterministic(self, valid_record: dict):
        notice = BzpNoticeBronze.model_validate(valid_record)
        out1 = to_bronze_output(notice)
        out2 = to_bronze_output(notice)
        assert out1.htmlBodySha256 == out2.htmlBodySha256

    def test_contractors_preserved_in_output(self, record_with_contractors: dict):
        notice = BzpNoticeBronze.model_validate(record_with_contractors)
        out = to_bronze_output(notice)
        assert out.contractors is not None
        assert out.contractors[0].contractorName == notice.contractors[0].contractorName


class TestNoticeRecordHash:
    def test_record_hash_is_deterministic(self, valid_record: dict):
        notice = BzpNoticeBronze.model_validate(valid_record)
        h1 = notice_record_hash(notice)
        h2 = notice_record_hash(notice)
        assert h1 == h2
        assert len(h1) == 64

    def test_record_hash_changes_when_payload_changes(self, valid_record: dict):
        notice1 = BzpNoticeBronze.model_validate(valid_record)
        changed = deepcopy(valid_record)
        changed["organizationName"] = changed["organizationName"] + " X"
        notice2 = BzpNoticeBronze.model_validate(changed)
        assert notice_record_hash(notice1) != notice_record_hash(notice2)
