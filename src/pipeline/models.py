"""
Cluster pipeline data model.

All structures use plain lists/dicts for JSON serialisability.
No frozensets required.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import NamedTuple


class ParsedName(NamedTuple):
    title:    str
    first:    str
    middle:   tuple[str, ...]   # ordered; () when empty
    last:     str
    suffix:   str
    nickname: str

    def to_dict(self) -> dict:
        return {**self._asdict(), "middle": list(self.middle)}

    @classmethod
    def from_dict(cls, d: dict) -> ParsedName:
        return cls(
            title=d["title"], first=d["first"],
            middle=tuple(d["middle"]),
            last=d["last"], suffix=d["suffix"], nickname=d["nickname"],
        )


@dataclass
class NameForm:
    arc:     ParsedName   # last = ARC family_name column verbatim (post-nominals stripped)
    norm_np: ParsedName   # strip_parens+strip_diacriticals → HumanName → lowercase

    def to_dict(self) -> dict:
        return {"arc": self.arc.to_dict(), "norm_np": self.norm_np.to_dict()}

    @classmethod
    def from_dict(cls, d: dict) -> NameForm:
        return cls(
            arc=ParsedName.from_dict(d["arc"]),
            norm_np=ParsedName.from_dict(d["norm_np"]),
        )


@dataclass
class InvestigatorRecord:
    grant_code:  str
    arc_first:   str        # exact ARC field (post-nominals already stripped)
    arc_family:  str        # exact ARC field
    orcid:       str | None
    grant_heps:  list[str]  # HEP codes for this grant (from institution_concordance)
    for_2d:      list[str]  # 2-digit FoR codes for this grant
    role_code:   str

    def to_dict(self) -> dict:
        return {
            "grant_code": self.grant_code,
            "arc_first":  self.arc_first,
            "arc_family": self.arc_family,
            "orcid":      self.orcid,
            "grant_heps": self.grant_heps,
            "for_2d":     self.for_2d,
            "role_code":  self.role_code,
        }

    @classmethod
    def from_dict(cls, d: dict) -> InvestigatorRecord:
        return cls(**d)


class ClusterStatus(str, Enum):
    UNRESOLVED  = "UNRESOLVED"
    SPLIT       = "SPLIT"
    MATCHED     = "MATCHED"
    ABSENT      = "ABSENT"
    UNDECIDABLE = "UNDECIDABLE"


class SplitReason(str, Enum):
    ORCID_BOTH_IN_OAX       = "ORCID_BOTH_IN_OAX"
    OVERLAPPING_FELLOWSHIPS = "OVERLAPPING_FELLOWSHIPS"
    MANUAL                  = "MANUAL"


@dataclass
class Cluster:
    cluster_id:  int
    name_forms:  list[NameForm]
    records:     list[InvestigatorRecord]

    # Aggregated (derived from records; kept for fast lookup without iterating records)
    orcids:       list[str]   # distinct non-null ORCIDs
    institutions: list[str]   # distinct HEP codes
    for_2d:       list[str]   # distinct 2-digit FoR codes
    co_awardee_cluster_ids: list[int]

    # Lineage — set on child clusters at split time
    parent_id:           int | None  = None
    sibling_cluster_ids: list[int]   = field(default_factory=list)
    split_reason:        str | None  = None   # SplitReason value or None

    # Resolution
    status:  str        = ClusterStatus.UNRESOLVED.value
    oax_id:  str | None = None
    tier:    int | None = None

    def to_dict(self) -> dict:
        return {
            "cluster_id":             self.cluster_id,
            "name_forms":             [nf.to_dict() for nf in self.name_forms],
            "records":                [r.to_dict() for r in self.records],
            "orcids":                 self.orcids,
            "institutions":           self.institutions,
            "for_2d":                 self.for_2d,
            "co_awardee_cluster_ids": self.co_awardee_cluster_ids,
            "parent_id":              self.parent_id,
            "sibling_cluster_ids":    self.sibling_cluster_ids,
            "split_reason":           self.split_reason,
            "status":                 self.status,
            "oax_id":                 self.oax_id,
            "tier":                   self.tier,
        }

    @classmethod
    def from_dict(cls, d: dict) -> Cluster:
        return cls(
            cluster_id=d["cluster_id"],
            name_forms=[NameForm.from_dict(nf) for nf in d["name_forms"]],
            records=[InvestigatorRecord.from_dict(r) for r in d["records"]],
            orcids=d["orcids"],
            institutions=d["institutions"],
            for_2d=d["for_2d"],
            co_awardee_cluster_ids=d["co_awardee_cluster_ids"],
            parent_id=d.get("parent_id"),
            sibling_cluster_ids=d.get("sibling_cluster_ids", []),
            split_reason=d.get("split_reason"),
            status=d.get("status", ClusterStatus.UNRESOLVED.value),
            oax_id=d.get("oax_id"),
            tier=d.get("tier"),
        )


# ── I/O helpers ───────────────────────────────────────────────────────────────

def save_clusters(clusters: list[Cluster], path: Path) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for c in clusters:
            f.write(json.dumps(c.to_dict(), ensure_ascii=False) + "\n")


def load_clusters(path: Path) -> list[Cluster]:
    clusters = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                clusters.append(Cluster.from_dict(json.loads(line)))
    return clusters
