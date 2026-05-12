"""
Match scoring constants for the cluster disambiguation pipeline.

A candidate OAX author is accepted when:
  - total score >= ACCEPT_THRESHOLD, AND
  - at least MIN_INDICATORS distinct indicators contributed

See docs/cluster_pipeline_design.md for rationale.
"""

ACCEPT_THRESHOLD = 100
MIN_INDICATORS   = 2

# Indicator point values
ORCID            = 90
FULL_FIRST       = 55   # norm_np first tokens identical
INITIAL_ONLY     = 20   # full first absent on one or both sides
INSTITUTION      = 45   # cluster HEP code in OAX affiliations
FOR_ONE_FIELD    = 15   # >= 1 matching 2-digit FoR code
FOR_THREE_FIELDS = 25   # >= 3 matching 2-digit FoR codes (replaces FOR_ONE_FIELD)
UNIQUE_IN_POOL   = 30   # only one candidate passes the name-compatibility gate
CO_AWARDEE_INST  = 25   # candidate shares ≥1 HEP with a matched co-awardee
CO_AWARDEE_FIELD = 15   # candidate shares ≥1 OAX topic field with a matched co-awardee
