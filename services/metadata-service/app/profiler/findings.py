from __future__ import annotations

from dataclasses import asdict, dataclass, field

import pandas as pd


@dataclass
class Finding:
    """One observation about the data. `rule` names the DQ rule in
    config/rules/quality_rules.yaml that enforces it, when one exists."""

    check: str
    severity: str  # info | warning | error
    count: int
    total: int
    detail: str
    rule: str | None = None
    sample: list[dict] = field(default_factory=list)

    @property
    def pct(self) -> float:
        return round(100 * self.count / self.total, 4) if self.total else 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["pct"] = self.pct
        return d


def sample_rows(df: pd.DataFrame, mask: pd.Series, cols: list[str] | None = None, n: int = 5) -> list[dict]:
    rows = df.loc[mask, cols or df.columns].head(n)
    return rows.astype(str).to_dict(orient="records")
