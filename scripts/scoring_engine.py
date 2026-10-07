"""Le barème d'Overtaker, porté depuis Services/ScoringEngine.swift.

C'est une RÉPLIQUE, pas une réécriture : tant que les clients de la 4.x scorent
encore de leur côté, les deux implémentations écrivent le même enregistrement
(`v2_<userId>_<eventId>_<type>_<league>`) avec une politique « tous les champs ».
Si elles divergent d'un seul point, le score se met à osciller selon qui a écrit
en dernier. Toute évolution du barème se fait donc des deux côtés à la fois, et
`test_scoring_engine.py` rejoue les mêmes attentes que
`Scripts/ScoringEngineCheck.swift`.

Référence : Services/ScoringEngine.swift (version 2).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional

VERSION = 2

# Valeurs brutes de PredictionType (Models/PredictionModels.swift).
RACE = "Race"
QUALIFYING = "Qualifying"
SPRINT_QUALIFYING = "SprintQualifying"
SPRINT = "Sprint"
FASTEST_LAP = "FastestLap"
FIRST_RETIREMENT = "FirstRetirement"
SEASON_DRIVER = "SeasonDriver"
SEASON_CONSTRUCTOR = "SeasonConstructor"
UNKNOWN = "Unknown"


def _round_half_away_from_zero(x: float) -> int:
    """L'arrondi de Swift, pas celui de Python.

    `Double.rounded()` arronditt .5 en s'éloignant de zéro ; `round()` en Python
    arrondit au pair le plus proche. Sur le barème sprint, 8 * 0.6 = 4.8 n'est pas
    concerné, mais la règle doit tenir pour toute valeur future tombant sur .5 —
    sinon le serveur et l'app ne donnent pas le même nombre.
    """
    return int(math.floor(x + 0.5)) if x >= 0 else -int(math.floor(-x + 0.5))


@dataclass(frozen=True)
class Rules:
    podium_exact: List[int]
    podium_misplaced: int
    podium_perfect_bonus: int
    pole_exact: int
    pole_front_row: int
    fastest_lap: int
    first_retirement_exact: int
    first_retirement_any_dnf: int

    def scaled(self, factor: float) -> "Rules":
        def s(v: int) -> int:
            return max(1, _round_half_away_from_zero(v * factor))

        return Rules(
            podium_exact=[s(v) for v in self.podium_exact],
            podium_misplaced=s(self.podium_misplaced),
            podium_perfect_bonus=s(self.podium_perfect_bonus),
            pole_exact=s(self.pole_exact),
            pole_front_row=s(self.pole_front_row),
            fastest_lap=s(self.fastest_lap),
            first_retirement_exact=s(self.first_retirement_exact),
            first_retirement_any_dnf=s(self.first_retirement_any_dnf),
        )


GRAND_PRIX_RULES = Rules(
    podium_exact=[25, 18, 15],
    podium_misplaced=5,
    podium_perfect_bonus=15,
    pole_exact=25,
    pole_front_row=8,
    fastest_lap=15,
    first_retirement_exact=20,
    first_retirement_any_dnf=8,
)

SPRINT_RULES = GRAND_PRIX_RULES.scaled(0.6)


def rules_for(prediction_type: str) -> Rules:
    if prediction_type in (SPRINT, SPRINT_QUALIFYING):
        return SPRINT_RULES
    return GRAND_PRIX_RULES


@dataclass
class SessionOutcome:
    """Tout ce qu'un résultat de séance peut nous apprendre."""

    finishing_order: List[str] = field(default_factory=list)
    retirement_order: List[str] = field(default_factory=list)
    fastest_lap_driver_id: Optional[str] = None


@dataclass
class Line:
    driver_id: str
    slot: int
    points: int
    reason: str  # exact | misplaced | frontRow | anyDNF | perfectBonus | miss

    def as_dict(self) -> dict:
        # Les clés sont celles de `ScoringEngine.Line` côté Swift : l'app décode ce
        # JSON tel quel pour expliquer un score sans le recalculer.
        return {
            "driverId": self.driver_id,
            "slot": self.slot,
            "points": self.points,
            "reason": self.reason,
        }


@dataclass
class Breakdown:
    total: int
    lines: List[Line]

    def as_dict(self) -> dict:
        return {"total": self.total, "lines": [l.as_dict() for l in self.lines]}


ZERO = Breakdown(total=0, lines=[])


def score(prediction_type: str, selections: List[str], outcome: SessionOutcome) -> Breakdown:
    """Note une prédiction. On décide sur le TYPE, jamais sur le nombre de picks."""
    picks = [p for p in (selections or []) if p]
    if not picks:
        return Breakdown(total=0, lines=[])

    r = rules_for(prediction_type)

    if prediction_type in (QUALIFYING, SPRINT_QUALIFYING):
        return _score_pole(picks, outcome, r)
    if prediction_type in (RACE, SPRINT):
        return _score_podium(picks, outcome, r)
    if prediction_type == FASTEST_LAP:
        if not outcome.fastest_lap_driver_id:
            return Breakdown(total=0, lines=[])
        hit = picks[0] == outcome.fastest_lap_driver_id
        pts = r.fastest_lap if hit else 0
        return Breakdown(total=pts,
                         lines=[Line(picks[0], 0, pts, "exact" if hit else "miss")])
    if prediction_type == FIRST_RETIREMENT:
        return _score_first_retirement(picks[0], outcome, r)

    # seasonDriver / seasonConstructor / unknown : jamais proposés, notés zéro.
    return Breakdown(total=0, lines=[])


def _score_pole(picks: List[str], outcome: SessionOutcome, r: Rules) -> Breakdown:
    pick = picks[0]
    order = outcome.finishing_order
    if order and order[0] == pick:
        return Breakdown(r.pole_exact, [Line(pick, 0, r.pole_exact, "exact")])
    if len(order) > 1 and order[1] == pick:
        return Breakdown(r.pole_front_row, [Line(pick, 0, r.pole_front_row, "frontRow")])
    return Breakdown(0, [Line(pick, 0, 0, "miss")])


def _score_podium(picks: List[str], outcome: SessionOutcome, r: Rules) -> Breakdown:
    podium = outcome.finishing_order[:3]
    if not podium:
        return Breakdown(total=0, lines=[])

    lines: List[Line] = []
    total = 0
    exact_count = 0

    for slot, driver in enumerate(picks[:3]):
        if slot < len(podium) and podium[slot] == driver:
            pts = r.podium_exact[slot] if slot < len(r.podium_exact) else 0
            total += pts
            exact_count += 1
            lines.append(Line(driver, slot, pts, "exact"))
        elif driver in podium:
            total += r.podium_misplaced
            lines.append(Line(driver, slot, r.podium_misplaced, "misplaced"))
        else:
            lines.append(Line(driver, slot, 0, "miss"))

    if exact_count == 3:
        total += r.podium_perfect_bonus
        lines.append(Line("", -1, r.podium_perfect_bonus, "perfectBonus"))

    return Breakdown(total, lines)


def _score_first_retirement(pick: str, outcome: SessionOutcome, r: Rules) -> Breakdown:
    if not outcome.retirement_order:
        return Breakdown(total=0, lines=[])
    if outcome.retirement_order[0] == pick:
        return Breakdown(r.first_retirement_exact,
                         [Line(pick, 0, r.first_retirement_exact, "exact")])
    if pick in outcome.retirement_order:
        return Breakdown(r.first_retirement_any_dnf,
                         [Line(pick, 0, r.first_retirement_any_dnf, "anyDNF")])
    return Breakdown(0, [Line(pick, 0, 0, "miss")])
