# Public Procurement Procedures under Polish Pzp (Conceptual Overview)

## Legal Context

Public procurement procedures in Poland are regulated by the Public
Procurement Law (Prawo zamówień publicznych -- Pzp, Act of 11 September
2019).

Each procedure type corresponds to a specific legal basis in the Act.\
In structured datasets (e.g., ENUM.019.json), every procedure variant is
mapped to a distinct code and explicitly defined legal basis.\
This means that even when procedures have similar names, they may differ
in their statutory foundation and therefore appear under separate
identifiers in the enumeration.

------------------------------------------------------------------------

# 1. Basic Procedure (Tryb podstawowy)

The Basic Procedure applies to contracts below EU thresholds.

It has three variants: 1. Without negotiations\
2. With optional negotiations\
3. With mandatory negotiations

Conceptually: - It is the default procedure for national-level
procurements. - It allows greater flexibility than the EU-level open
procedure. - It may include negotiations depending on the chosen
variant.

Each variant has a separate legal basis and therefore a separate code in
ENUM.019.json.

------------------------------------------------------------------------

# 2. Open Procedure (Przetarg nieograniczony)

This is the standard EU-level competitive procedure.

Key characteristics: - Any interested economic operator may submit a
tender. - No negotiations. - Single-stage submission of tenders. - High
level of formalization and transparency.

It is typically used for contracts above EU thresholds.

In ENUM.019.json, this procedure appears under different codes depending
on: - Whether the contract is above EU thresholds, - Whether it concerns
a framework agreement, - Whether it is sectoral or defense-related.

------------------------------------------------------------------------

# 3. Restricted Procedure (Przetarg ograniczony)

A two-stage competitive procedure:

Stage 1: Economic operators apply for participation.\
Stage 2: Only selected candidates are invited to submit tenders.

Used when: - The contracting authority wants to limit the number of
bidders, - The subject matter is complex, - Qualification filtering is
needed.

Each legal configuration (classic, sectoral, framework agreement, etc.)
has its own code in ENUM.019.json.

------------------------------------------------------------------------

# 4. Competitive Procedure with Negotiation (Negocjacje z ogłoszeniem)

A competitive procedure that includes negotiations after an initial
selection phase.

Typical structure: - Publication of a notice, - Requests to
participate, - Negotiations with selected candidates, - Final tenders.

Used when: - The contracting authority cannot precisely define technical
or legal solutions, - Adaptation or negotiation is necessary.

Different statutory grounds are represented by separate legal references
and separate ENUM codes.

------------------------------------------------------------------------

# 5. Competitive Dialogue (Dialog konkurencyjny)

Designed for highly complex contracts.

Structure: 1. Publication and selection of candidates, 2. Dialogue phase
to identify suitable solutions, 3. Submission of final tenders.

Used particularly for large infrastructure or innovative projects.

Each legal regime (classic, sectoral, defense) results in a distinct
legal basis and separate enumeration code.

------------------------------------------------------------------------

# 6. Innovation Partnership (Partnerstwo innowacyjne)

A special procedure combining: - Research and development, - Subsequent
purchase of the developed solution.

Used when: - The required product or service does not yet exist on the
market.

Its legal foundation differs depending on thresholds and regimes, which
is reflected in separate ENUM identifiers.

------------------------------------------------------------------------

# 7. Negotiated Procedure without Prior Publication (Negocjacje bez ogłoszenia)

A non-competitive procedure.

Characteristics: - No public contract notice, - Selected economic
operators are invited directly, - Negotiations are conducted.

Allowed only in strictly defined exceptional circumstances.

Each statutory justification corresponds to a distinct legal basis and
ENUM code.

------------------------------------------------------------------------

# 8. Single-Source Procurement (Zamówienie z wolnej ręki)

The most exceptional procedure.

Characteristics: - No competition, - Direct negotiation with a single
economic operator.

Used only when strict legal conditions are met (e.g., exclusive rights,
extreme urgency).

Each justification scenario is legally distinct and mapped to a specific
ENUM.019.json code.

------------------------------------------------------------------------

# 9. Competition Procedure (Konkurs)

Used to select the best design or concept (e.g., architectural
competition).

It does not directly award a contract for works or services, but may
lead to a negotiated procedure with the winner.

Different legal bases for competitions are also represented as distinct
ENUM identifiers.

------------------------------------------------------------------------

# Conceptual Classification by Level of Competition

Fully Competitive: - Open Procedure - Restricted Procedure - Basic
Procedure (without negotiations)

Competitive with Negotiation Elements: - Basic Procedure (with
negotiations) - Competitive Procedure with Negotiation - Competitive
Dialogue - Innovation Partnership

Non-Competitive / Exceptional: - Negotiated Procedure without
Publication - Single-Source Procurement

------------------------------------------------------------------------

# Analytical Implication

Although procedures may appear conceptually similar, each: - Operates
under a specific legal article, - May apply to different value
thresholds, - May belong to different regimes (classic, sectoral,
defense), - And therefore has a distinct code in ENUM.019.json.

For analytical modeling, it is recommended to separate:

1.  Conceptual Procedure Type (e.g., Open, Restricted, Basic)\
2.  Legal Basis Variant (as defined in ENUM.019.json)\
3.  Regime (Classic / Sectoral / Defense)\
4.  Threshold Level (Below EU / Above EU)

This layered approach allows proper normalization while preserving legal
precision.
