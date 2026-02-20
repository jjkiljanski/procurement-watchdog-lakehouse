# Types of Contracting Authorities under Polish Public Procurement Law (Pzp)

## Legal Framework

These categories are defined under the Polish Public Procurement Law
(Prawo zamówień publicznych -- Pzp, Act of 11 September 2019).

The Polish system distinguishes between three main types of contracting
authorities:

1.  Zamawiający klasyczny (Classic Contracting Authority)\
2.  Zamawiający sektorowy (Sectoral Contracting Authority)\
3.  Zamawiający subsydiowany (Subsidized Contracting Authority)

------------------------------------------------------------------------

# 1. Zamawiający klasyczny (Classic Contracting Authority)

A Zamawiający klasyczny is a public contracting authority.

This includes: - State authorities (ministries, central government
bodies) - Local government units (municipalities, counties, regions) -
Public sector entities - Other entities established to meet needs in the
general interest and financed or controlled by the state

Key characteristics: - Subject to Pzp for all procurements above
statutory thresholds - Operates under the general (classic) procurement
regime - Applies standard EU and national thresholds

This is the default and most common category of contracting authority.

------------------------------------------------------------------------

# 2. Zamawiający sektorowy (Sectoral Contracting Authority)

A Zamawiający sektorowy operates in one of the so-called utilities
sectors, such as: - Energy (electricity, gas, heat) - Water supply and
wastewater - Public transport (rail, metro, tram) - Ports and airports -
Postal services - Extraction of oil, gas, coal

Polish law distinguishes three subcategories under Article 5(1) Pzp:

## 2.1 Art. 5(1)(1) -- Public entity performing sectoral activity

A public entity that conducts sectoral activities.

Example: a municipally owned water utility company.

## 2.2 Art. 5(1)(2) -- Private entity with special or exclusive rights

A private entity that performs sectoral activity based on special or
exclusive rights granted by a public authority.

These rights limit market access and typically create monopoly or
quasi-monopoly conditions.

## 2.3 Art. 5(1)(3) -- Controlled entity performing sectoral activity

An entity controlled by a sectoral contracting authority and carrying
out sectoral activities.

Control may involve: - Majority ownership - Dominant influence - Power
to appoint management bodies

Key characteristics of sectoral contracting authorities: - Different
(usually higher) EU thresholds - Greater procedural flexibility -
Broader use of negotiated procedures - Ability to establish
qualification systems

------------------------------------------------------------------------

# 3. Zamawiający subsydiowany (Subsidized Contracting Authority)

A Zamawiający subsydiowany is not inherently a public or sectoral
contracting authority.

It becomes subject to Pzp only when: - A specific contract is financed
in more than 50% by public funds, and - The contract concerns certain
categories of works or related services (typically infrastructure
projects)

Important characteristics: - The status applies per procurement, not
permanently - The entity may be private (e.g., foundation, association,
private company) - The obligation arises solely due to the level of
public funding

The rationale behind this category is to ensure transparency and
competition in the use of public funds, even when they are granted to
private entities.

------------------------------------------------------------------------

## Comparative Overview

| Feature | Zamawiający klasyczny (Classic) | Zamawiający sektorowy (Sectoral) | Zamawiający subsydiowany (Subsidized) |
|---|---|---|---|
| Public status required | Yes | Sometimes | No |
| Operates in utilities sector | No | Yes | No |
| Obligation applies permanently | Yes | Yes | No (per contract only) |
| Triggered by public funding level | No | No | Yes (>50%) |
| Procedural flexibility | Standard regime | More flexible | Standard regime |


# Suggested Data Modeling Approach

In analytical systems, contracting authority type may be modeled as:

ContractingAuthorityType: - Classic - Sectoral - Subsidized
(procurement-level attribute)

For sectoral authorities, an additional attribute may specify the legal
basis: - Sectoral_Public (Art. 5(1)(1)) - Sectoral_ExclusiveRights (Art.
5(1)(2)) - Sectoral_Controlled (Art. 5(1)(3))

For subsidized authorities, it is recommended to model the status at the
procurement level rather than at the entity level.
