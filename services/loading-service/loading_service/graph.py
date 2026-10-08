"""Turn the active control rows into steps and dependencies for the scheduler."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Step:
    layer: str
    id: str

    @property
    def task_id(self) -> str:
        return f"{self.layer}__{self.id}"


def build_graph(bronze: list[dict], silver: list[dict], gold: list[dict]):
    """Returns (steps, edges) where each edge is (upstream, downstream)."""
    steps = [Step("bronze", b["id"]) for b in bronze]
    steps += [Step("silver", s["id"]) for s in silver]
    steps += [Step("gold", g["id"]) for g in gold]
    known = set(steps)
    edges = []
    for s in silver:
        up = Step("bronze", s["bronze_id"])
        if up in known:
            edges.append((up, Step("silver", s["id"])))
    for g in gold:
        for dep in g["depends_on"]:
            up = Step("silver", dep)
            if up in known:
                edges.append((up, Step("gold", g["id"])))
    return steps, edges
