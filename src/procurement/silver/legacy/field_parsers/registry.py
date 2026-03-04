"""Registry for notice-type specific parsed-value extractors."""

from __future__ import annotations

from typing import Callable

from procurement.silver.field_parsers.agreement_intention_notice import (
    parse_agreement_intention_notice,
)
from procurement.silver.field_parsers.agreement_update_notice import parse_agreement_update_notice
from procurement.silver.field_parsers.circumstances_fulfillment_notice import (
    parse_circumstances_fulfillment_notice,
)
from procurement.silver.field_parsers.competition_notice import parse_competition_notice
from procurement.silver.field_parsers.competition_result_notice import (
    parse_competition_result_notice,
)
from procurement.silver.field_parsers.concession_agreement_notice import (
    parse_concession_agreement_notice,
)
from procurement.silver.field_parsers.concession_intention_agreement_notice import (
    parse_concession_intention_agreement_notice,
)
from procurement.silver.field_parsers.concession_notice import parse_concession_notice
from procurement.silver.field_parsers.concession_update_agreement_notice import (
    parse_concession_update_agreement_notice,
)
from procurement.silver.field_parsers.contract_notice import parse_contract_notice
from procurement.silver.field_parsers.contract_performing_notice import (
    parse_contract_performing_notice,
)
from procurement.silver.field_parsers.notice_update_concession import (
    parse_notice_update_concession,
)
from procurement.silver.field_parsers.notice_update_notice import parse_notice_update_notice
from procurement.silver.field_parsers.small_contract_notice import (
    parse_small_contract_notice,
)
from procurement.silver.field_parsers.tender_result_notice import parse_tender_result_notice
from procurement.silver.field_parsers.types import ParsedValues

NoticeParserFn = Callable[..., ParsedValues]


PARSER_REGISTRY: dict[str, NoticeParserFn] = {
    "AgreementIntentionNotice": parse_agreement_intention_notice,
    "AgreementUpdateNotice": parse_agreement_update_notice,
    "CircumstancesFulfillmentNotice": parse_circumstances_fulfillment_notice,
    "CompetitionNotice": parse_competition_notice,
    "CompetitionResultNotice": parse_competition_result_notice,
    "ConcessionAgreementNotice": parse_concession_agreement_notice,
    "ConcessionIntentionAgreementNotice": parse_concession_intention_agreement_notice,
    "ConcessionNotice": parse_concession_notice,
    "ConcessionUpdateAgreementNotice": parse_concession_update_agreement_notice,
    "ContractNotice": parse_contract_notice,
    "ContractPerformingNotice": parse_contract_performing_notice,
    "NoticeUpdateConcession": parse_notice_update_concession,
    "NoticeUpdateNotice": parse_notice_update_notice,
    "SmallContractNotice": parse_small_contract_notice,
    "TenderResultNotice": parse_tender_result_notice,
}


def parse_notice_values(
    *,
    notice_type: str | None,
    sections_model,
    soup,
    procedure_result: str | None = None,
) -> ParsedValues:
    """Dispatch notice value parsing using the registry."""
    if not notice_type:
        return {}
    parser = PARSER_REGISTRY.get(notice_type)
    if parser is None:
        return {}
    return parser(
        sections_model=sections_model,
        soup=soup,
        procedure_result=procedure_result,
    )
