#!/usr/bin/env python3
"""Calcule les scores de pronostics côté serveur et les écrit dans CloudKit.

POURQUOI CE SCRIPT EXISTE
-------------------------
Jusqu'ici, chaque téléphone scorait **tous les joueurs** : `ScoringService.scoreEvent`
lit les pronostics de tout le monde et écrit leurs `UserScoreV2`. Ça impose de donner
à n'importe quel utilisateur authentifié le droit d'écrire le score des autres, ça
refait le même calcul autant de fois qu'il y a d'appareils, et ça fait dépendre le
classement d'une ligue de qui a ouvert l'app en dernier. Le serveur, lui, voit les
résultats dès qu'il les publie et n'a besoin d'aucun droit côté client.

PARITÉ AVEC LE CLIENT — à lire avant de "l'améliorer"
-----------------------------------------------------
Tant que des clients 4.x scorent encore, les deux écrivent le même enregistrement.
Pour qu'ils ne se contredisent pas, ce script reproduit le client À L'IDENTIQUE, y
compris dans ce qu'il ne sait pas faire :

  * `ScoringService` ne renseigne ni l'ordre des abandons ni l'auteur du meilleur
    tour, donc les pronostics « premier abandon » et « meilleur tour » valent zéro
    chez lui. Le serveur peut les calculer — `--score-extras` le fait — mais tant
    que le client écrit, l'activer déclencherait une oscillation : le serveur écrit
    20 points, le premier téléphone qui se lance réécrit 0.
  * l'avatar est repris du profil, comme le client le fait, sinon chaque écriture
    serveur effacerait l'avatar du classement (`forceReplace` remplace tout).

Quand la version qui retire le scoring client sera majoritaire : passer
`--score-extras`, puis retirer `write` à `_icloud` sur `UserScoreV2` dans la console
CloudKit. Ce script continuera de fonctionner : une clé server-to-server n'est
soumise à aucun rôle.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, Iterable, List, Optional, Tuple

import cloudkit_client
import scoring_engine as engine

DATA_DIR = os.environ.get("DATA_DIR", "data")

# Suffixe d'eventId par type de séance, côté app (F1Event.eventId).
TYPE_SUFFIX = {
    "grand_prix": "gp",
    "qualifying": "quali",
    "sprint": "sprint",
    "sprint_qualifying": "sprint_quali",
}

# Quel type de pronostic se note sur quelle séance.
PREDICTION_TYPES_FOR_SUFFIX = {
    "gp": [engine.RACE, engine.FASTEST_LAP, engine.FIRST_RETIREMENT],
    "quali": [engine.QUALIFYING],
    "sprint": [engine.SPRINT],
    "sprint_quali": [engine.SPRINT_QUALIFYING],
}


# --------------------------------------------------------------------------
# Identifiants d'évènement — réplique de F1Event.raceSlug / F1Event.eventId
# --------------------------------------------------------------------------

def race_slug(race_name: str) -> str:
    """Le seul endroit où un nom de course devient un fragment d'identifiant.

    Copie conforme de `F1Event.raceSlug`. L'ordre des remplacements compte : le
    tiret devient un souligné AVANT que « grand_prix » ne soit abrégé, sinon
    « Barcelona-Catalonia Grand Prix » ne se recolle pas.
    """
    return (race_name.lower()
            .replace(" ", "_")
            .replace("'", "")
            .replace("-", "_")
            .replace("grand_prix", "gp"))


def event_id(race_name: str, suffix: str) -> str:
    return f"{race_slug(race_name)}_{suffix}".replace("__", "_").strip("_")


# --------------------------------------------------------------------------
# Résultats publiés
# --------------------------------------------------------------------------

def _load(name: str) -> dict:
    path = os.path.join(DATA_DIR, name)
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _races(doc: dict) -> List[dict]:
    return doc.get("MRData", {}).get("RaceTable", {}).get("Races", []) or []


def _finishing_order(rows: Iterable[dict]) -> List[str]:
    """Ordre d'arrivée, pilote par pilote — comme `extractDriverIds` côté app."""
    def pos(row: dict) -> int:
        try:
            return int(row.get("position", 999))
        except (TypeError, ValueError):
            return 999
    return [r["Driver"]["driverId"] for r in sorted(rows, key=pos) if r.get("Driver")]


def _retirement_order(rows: Iterable[dict]) -> List[str]:
    """Pilotes ayant abandonné, le premier sorti en tête.

    Ergast ne donne pas l'heure de l'abandon : on la déduit du nombre de tours
    bouclés, le moins avancé étant sorti le premier. Un `status` qui commence par
    « + » (« +1 Lap ») est une arrivée, pas un abandon ; « Finished » non plus.
    """
    out = []
    for r in rows:
        status = (r.get("status") or "").strip()
        if status == "Finished" or status.startswith("+"):
            continue
        if not r.get("Driver"):
            continue
        try:
            laps = int(r.get("laps", 0))
        except (TypeError, ValueError):
            laps = 0
        out.append((laps, r["Driver"]["driverId"]))
    return [d for _, d in sorted(out, key=lambda x: x[0])]


def _fastest_lap_driver(rows: Iterable[dict]) -> Optional[str]:
    for r in rows:
        fl = r.get("FastestLap") or {}
        if str(fl.get("rank")) == "1" and r.get("Driver"):
            return r["Driver"]["driverId"]
    return None


def _outcomes_by_race(score_extras: bool):
    """Deux index par séance : par slug de nom de course, et par date de course.

    La date est l'index principal, parce que les noms ne concordent pas toujours :
    le calendrier dit « Barcelona-Catalonia Grand Prix » là où le flux de résultats
    dit « Barcelona Grand Prix ». Aucune inclusion ne rapproche ces deux chaînes, si
    bien que ce Grand Prix n'a jamais été scoré — ni ici, ni par l'app, qui fait la
    même comparaison dans `ScoringService.fetchRaceResults`. Une date de course est
    la même des deux côtés.
    """
    by_suffix: Dict[str, Dict[str, engine.SessionOutcome]] = {
        "gp": {}, "quali": {}, "sprint": {}, "sprint_quali": {},
    }
    by_date: Dict[str, Dict[str, engine.SessionOutcome]] = {
        "gp": {}, "quali": {}, "sprint": {}, "sprint_quali": {},
    }

    def put(suffix: str, race: dict, outcome: engine.SessionOutcome):
        if not outcome.finishing_order:
            return
        by_suffix[suffix][race_slug(race["raceName"])] = outcome
        date = (race.get("date") or "")[:10]
        if date:
            by_date[suffix][date] = outcome

    for race in _races(_load("current_results.json")):
        rows = race.get("Results") or []
        put("gp", race, engine.SessionOutcome(
            finishing_order=_finishing_order(rows),
            retirement_order=_retirement_order(rows) if score_extras else [],
            fastest_lap_driver_id=_fastest_lap_driver(rows) if score_extras else None,
        ))

    for race in _races(_load("qualifying.json")):
        put("quali", race, engine.SessionOutcome(
            finishing_order=_finishing_order(race.get("QualifyingResults") or [])))

    for race in _races(_load("sprint.json")):
        rows = race.get("SprintResults") or []
        put("sprint", race, engine.SessionOutcome(
            finishing_order=_finishing_order(rows),
            retirement_order=_retirement_order(rows) if score_extras else [],
        ))

    # sprint_qualifying.json ne porte qu'un numéro de manche : le nom vient des
    # résultats de course, qui portent le même `round`.
    round_names = {str(r.get("round")): r["raceName"]
                   for r in _races(_load("current_results.json"))}
    round_dates = {str(r.get("round")): (r.get("date") or "")[:10]
                   for r in _races(_load("current_results.json"))}
    try:
        sq_doc = _load("sprint_qualifying.json")
    except FileNotFoundError:
        sq_doc = {}
    sq_races = _races(sq_doc) or sq_doc.get("rounds") or []
    for race in sq_races:
        name = race.get("raceName") or round_names.get(str(race.get("round")))
        if not name:
            continue
        put("sprint_quali", dict(race, raceName=name,
                                 date=race.get("date") or round_dates.get(str(race.get("round")), "")),
            engine.SessionOutcome(
                finishing_order=_finishing_order(race.get("SprintQualifyingResults") or [])))

    return by_suffix, by_date


def _match(race_part: str, candidates: Dict[str, engine.SessionOutcome]):
    """La correspondance approximative de `ScoringService.fetchRaceResults`.

    Le calendrier dit « Bahrain Grand Prix », le flux de résultats « Bahrain Grand
    Prix in Malaysia » : un `==` ne rapprocherait jamais les deux, et cette course
    ne serait scorée pour personne. L'app teste l'inclusion dans les deux sens, on
    fait pareil — et on prend la correspondance la plus courte, pour qu'un nom qui
    en contient un autre ne vole pas ses résultats.
    """
    hits = [(slug, out) for slug, out in candidates.items()
            if race_part in slug or slug in race_part]
    if not hits:
        return None
    hits.sort(key=lambda kv: abs(len(kv[0]) - len(race_part)))
    return hits[0][1]


def build_outcomes(score_extras: bool) -> Dict[str, Tuple[str, engine.SessionOutcome]]:
    """eventId (tel que l'app le forge) -> (nom de course, résultat).

    On part du CALENDRIER, pas des résultats : c'est le calendrier qui a produit
    les `eventId` portés par les pronostics.
    """
    by_suffix, by_date = _outcomes_by_race(score_extras)
    calendar = _load("f1_2026_calendar.json")

    outcomes: Dict[str, Tuple[str, engine.SessionOutcome]] = {}
    for race in calendar.get("races", []):
        if race.get("cancelled"):
            continue

        # La date de la COURSE identifie le week-end dans les deux flux ; les
        # résultats de qualification et de sprint sont publiés sous cette même
        # date, pas sous la leur.
        gp_date = ""
        for event in race.get("events", []):
            if event.get("type") == "grand_prix":
                gp_date = (event.get("date_time") or "")[:10]
                break

        for event in race.get("events", []):
            suffix = TYPE_SUFFIX.get(event.get("type", ""))
            if not suffix:
                continue
            race_name = event.get("race_name") or race.get("name") or ""
            if not race_name:
                continue

            outcome = by_date[suffix].get(gp_date) if gp_date else None
            if outcome is None:
                outcome = _match(race_slug(race_name), by_suffix[suffix])
            if outcome and outcome.finishing_order:
                outcomes[event_id(race_name, suffix)] = (race_name, outcome)

    return outcomes


# --------------------------------------------------------------------------
# CloudKit
# --------------------------------------------------------------------------

def _string(record: dict, key: str) -> str:
    return (record.get("fields", {}).get(key, {}) or {}).get("value") or ""


def _string_list(record: dict, key: str) -> List[str]:
    value = (record.get("fields", {}).get(key, {}) or {}).get("value")
    return list(value) if isinstance(value, list) else []


def load_avatars(ck: cloudkit_client.CloudKit, user_ids: Iterable[str]) -> Dict[str, str]:
    """recordName de UserProfile -> avatarId, comme `ScoringService.avatarId(for:)`.

    Par lookup et non par requête : `UserProfile` n'a pas d'index queryable, et de
    toute façon seuls les profils ayant pronostiqué nous intéressent. Un profil
    supprimé ressort simplement absent — l'app retombe alors sur son avatar par
    défaut, exactement comme avant.
    """
    avatars: Dict[str, str] = {}
    for name, rec in ck.lookup(list(user_ids)).items():
        avatar = _string(rec, "avatarId")
        if avatar:
            avatars[name] = avatar
    return avatars


def predictions_for(ck: cloudkit_client.CloudKit, event: str) -> List[dict]:
    return list(ck.query(
        "Prediction",
        filters=[{"fieldName": "eventId", "comparator": "EQUALS",
                  "fieldValue": {"value": event}}],
    ))


def user_id_of(record: dict) -> str:
    """`userReference` est une référence ; son recordName EST l'identifiant joueur."""
    ref = (record.get("fields", {}).get("userReference", {}) or {}).get("value")
    if isinstance(ref, dict):
        return (ref.get("recordName")
                or (ref.get("record") or {}).get("recordName")
                or "")
    return ""


def modified_at(record: dict) -> int:
    """Horodatage de dernière modification, en millisecondes (0 si absent)."""
    for key in ("modified", "created"):
        stamp = (record.get(key) or {}).get("timestamp")
        if isinstance(stamp, (int, float)):
            return int(stamp)
    return 0


def score_record(user_id: str, display_name: str, avatar: Optional[str],
                 league: str, event: str, ptype: str,
                 breakdown: engine.Breakdown) -> dict:
    fields = {
        "scoringVersion": {"value": engine.VERSION},
        "breakdown": {"value": json.dumps(breakdown.as_dict(), separators=(",", ":"))},
        "userId": {"value": user_id},
        "userDisplayName": {"value": display_name},
        "leagueCode": {"value": league},
        "eventId": {"value": event},
        "predictionType": {"value": ptype},
        "pointsEarned": {"value": breakdown.total},
    }
    if avatar:
        fields["avatarId"] = {"value": avatar}
    return {
        "recordType": "UserScoreV2",
        # Même nom déterministe que l'app : `v2_` + userId_eventId_type_ligue.
        # Le préfixe est obligatoire — sans lui le nom entre en collision avec
        # l'enregistrement `UserScore` de la v1 et CloudKit refuse TOUTE écriture.
        "recordName": f"v2_{user_id}_{event}_{ptype}_{league}",
        "fields": fields,
    }


def compare(ck: cloudkit_client.CloudKit, computed: List[dict]) -> int:
    """Confronte le calcul serveur aux scores déjà écrits par les clients.

    C'est LE contrôle à passer avant la première écriture : tant que les
    téléphones scorent aussi, le moindre écart se traduirait par un score qui
    change de valeur selon qui a écrit en dernier. On veut donc zéro différence
    sur les enregistrements qui existent déjà — les manquants, eux, sont ce que
    le serveur vient combler.
    """
    existing = ck.lookup([r["recordName"] for r in computed])
    same = different = missing = 0
    diffs: List[str] = []

    for rec in computed:
        name = rec["recordName"]
        mine = rec["fields"]["pointsEarned"]["value"]
        found = existing.get(name)
        if not found:
            missing += 1
            continue
        theirs = (found.get("fields", {}).get("pointsEarned", {}) or {}).get("value")
        if theirs == mine:
            same += 1
        else:
            different += 1
            if len(diffs) < 20:
                diffs.append(f"  {name}\n      client={theirs}  serveur={mine}")

    print(f"identiques        : {same}")
    print(f"divergents        : {different}")
    print(f"absents (à créer) : {missing}")
    for d in diffs:
        print(d)
    return 1 if different else 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="calcule et affiche, n'écrit rien")
    parser.add_argument("--event", help="ne traiter qu'un eventId")
    parser.add_argument("--score-extras", action="store_true",
                        help="noter aussi abandons et meilleur tour "
                             "(à n'activer que lorsque les clients ne scorent plus)")
    parser.add_argument("--compare", action="store_true",
                        help="confronte le calcul aux scores déjà présents, "
                             "sans rien écrire")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    outcomes = build_outcomes(score_extras=args.score_extras)
    if args.event:
        outcomes = {k: v for k, v in outcomes.items() if k == args.event}
    if not outcomes:
        print("Aucune séance avec résultats. Rien à faire.")
        return 0

    print(f"{len(outcomes)} séance(s) avec résultats publiés.")

    ck = cloudkit_client.from_env()

    # On lit d'abord TOUS les pronostics concernés, puis les profils en une fois :
    # un lookup par joueur et par course ferait des centaines d'allers-retours.
    by_event: Dict[str, List[dict]] = {}
    for event in sorted(outcomes):
        preds = predictions_for(ck, event)
        if preds:
            by_event[event] = preds

    avatars = load_avatars(
        ck, {user_id_of(r) for preds in by_event.values() for r in preds})
    print(f"{len(avatars)} profil(s) avec avatar.")

    to_write: List[dict] = []
    examined = 0

    for event, preds in sorted(by_event.items()):
        race_name, outcome = outcomes[event]
        examined += len(preds)
        for rec in preds:
            ptype = _string(rec, "type")
            selections = _string_list(rec, "selections")
            league = _string(rec, "leagueCode")
            uid = user_id_of(rec)
            if not uid or not league:
                continue
            breakdown = engine.score(ptype, selections, outcome)
            built = score_record(
                uid, _string(rec, "userDisplayName") or "Unknown",
                avatars.get(uid), league, event, ptype, breakdown)
            built["_modified"] = modified_at(rec)
            to_write.append(built)
            if args.verbose:
                print(f"  {event:<28} {league:<8} {ptype:<16} "
                      f"{breakdown.total:>3} pts  {uid[:8]}")

    # Un même (joueur, séance, type, ligue) peut porter PLUSIEURS enregistrements
    # `Prediction` : l'app en crée un nouveau à chaque modification au lieu de
    # réutiliser le précédent, sauf pour la ligue GLOBAL. Ils se ramènent tous au
    # même nom de score, et CloudKit refuse un lot qui touche deux fois le même
    # enregistrement — « Record updated multiple times in one batch ». On garde le
    # plus récent, qui est le pronostic que le joueur a réellement laissé.
    deduped: Dict[str, dict] = {}
    for rec in to_write:
        name = rec["recordName"]
        previous = deduped.get(name)
        if previous is None or rec["_modified"] >= previous["_modified"]:
            deduped[name] = rec
    dropped = len(to_write) - len(deduped)
    to_write = [{k: v for k, v in r.items() if not k.startswith("_")}
                for r in deduped.values()]

    print(f"{examined} pronostic(s) lus, {len(to_write)} score(s) à écrire"
          + (f" ({dropped} doublon(s) de pronostic écartés)." if dropped else "."))

    if args.compare:
        return compare(ck, to_write)

    if args.dry_run:
        print("--dry-run : rien n'a été écrit.")
        return 0
    if not to_write:
        return 0

    result = ck.save(to_write)
    print(f"écrits : {result['saved']}, refusés : {result['failed']}")
    for err in result["errors"]:
        print(f"  refus : {err}")
    # Un refus doit faire échouer le job : c'est précisément le silence sur les
    # refus d'écriture qui a laissé la panne vivre une semaine côté app.
    return 1 if result["failed"] else 0


def main_for_job(score_extras: bool = False) -> int:
    """Point d'entrée pour le job de données, sans ligne de commande.

    Même chemin que `--dry-run` absent : on calcule et on écrit. Les exceptions
    remontent à l'appelant, qui les journalise sans interrompre la publication
    des données.
    """
    return main(["--score-extras"] if score_extras else [])


if __name__ == "__main__":
    sys.exit(main())
