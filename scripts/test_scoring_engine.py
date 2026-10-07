"""Les mêmes attentes que Scripts/ScoringEngineCheck.swift, rejouées en Python.

Ce fichier n'a d'intérêt que tant qu'il est le MIROIR EXACT du harnais Swift :
c'est lui qui garantit que le serveur et l'app attribuent le même nombre de
points à la même prédiction. Si une attente change d'un côté, elle change des
deux, dans le même commit.
"""

import sys

from scoring_engine import (
    FASTEST_LAP,
    FIRST_RETIREMENT,
    QUALIFYING,
    RACE,
    SPRINT,
    SPRINT_QUALIFYING,
    UNKNOWN,
    SessionOutcome,
    score,
)

FAILURES = 0


def expect(label: str, actual, expected):
    global FAILURES
    ok = actual == expected
    if not ok:
        FAILURES += 1
    print(f"  {'PASS' if ok else 'FAIL'} {label:<42} {actual!s:>4} (attendu {expected})")


podium = SessionOutcome(finishing_order=["ver", "nor", "lec", "ham", "rus", "pia"])
dnf = SessionOutcome(finishing_order=["ver", "nor", "lec"],
                     retirement_order=["alo", "str", "oco"])
fl = SessionOutcome(finishing_order=["ver", "nor", "lec"], fastest_lap_driver_id="nor")

print("Podium Grand Prix")
expect("perfect VER/NOR/LEC", score(RACE, ["ver", "nor", "lec"], podium).total, 73)
expect("2 exact, 3rd off podium", score(RACE, ["ver", "nor", "ham"], podium).total, 43)
expect("right 3, wrong order", score(RACE, ["lec", "ver", "nor"], podium).total, 15)
expect("P1 exact only", score(RACE, ["ver", "ham", "rus"], podium).total, 25)
expect("all wrong", score(RACE, ["ham", "rus", "pia"], podium).total, 0)

print("Podium sprint (60 %)")
expect("perfect", score(SPRINT, ["ver", "nor", "lec"], podium).total, 44)
expect("right 3, wrong order", score(SPRINT, ["lec", "ver", "nor"], podium).total, 9)

print("Pole")
expect("exact", score(QUALIFYING, ["ver"], podium).total, 25)
expect("front row (P2)", score(QUALIFYING, ["nor"], podium).total, 8)
expect("miss", score(QUALIFYING, ["ham"], podium).total, 0)
expect("sprint pole exact", score(SPRINT_QUALIFYING, ["ver"], podium).total, 15)

print("Premier abandon")
expect("exact (first out)", score(FIRST_RETIREMENT, ["alo"], dnf).total, 20)
expect("retired, not first", score(FIRST_RETIREMENT, ["str"], dnf).total, 8)
expect("driver finished", score(FIRST_RETIREMENT, ["ver"], dnf).total, 0)
expect("no retirement data", score(FIRST_RETIREMENT, ["alo"], podium).total, 0)

print("Meilleur tour")
expect("unscored when data absent", score(FASTEST_LAP, ["ver"], podium).total, 0)
expect("exact once data exists", score(FASTEST_LAP, ["nor"], fl).total, 15)

print("Dispatch sur le type, pas sur le nombre de picks")
expect("firstRetirement != pole", score(FIRST_RETIREMENT, ["nor"], podium).total, 0)

perfect = score(RACE, ["ver", "nor", "lec"], podium).total
scrambled = score(RACE, ["lec", "ver", "nor"], podium).total
expect("perfect > 4x scrambled", 1 if perfect > scrambled * 4 else 0, 1)
expect("unknown type is inert", score(UNKNOWN, ["ver"], podium).total, 0)

print("Détail (breakdown)")
b = score(RACE, ["ver", "nor", "lec"], podium)
expect("perfect podium -> 4 lignes", len(b.lines), 4)
expect("la 4e ligne est le bonus", b.lines[-1].reason, "perfectBonus")
expect("somme des lignes == total", sum(l.points for l in b.lines), b.total)
b2 = score(RACE, ["ver", "ham", "rus"], podium)
expect("somme des lignes == total (partiel)", sum(l.points for l in b2.lines), b2.total)

print()
if FAILURES:
    print(f"{FAILURES} ÉCHEC(S)")
    sys.exit(1)
print("Tout passe — le barème Python est identique au barème Swift.")
