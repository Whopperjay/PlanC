"""Accès serveur à la base publique CloudKit (CloudKit Web Services).

Une clé *server-to-server* n'est pas un utilisateur : elle n'est soumise à aucun
des rôles `_world` / `_icloud` / `_creator`, et la console le dit sans détour —
« unrestricted access to your public database ». C'est précisément ce qui permet
de retirer aux clients le droit d'écrire le score des autres joueurs une fois que
ce script est seul à l'écrire.

Signature (v1) : on signe la chaîne

    <date ISO8601>:<base64(SHA-256(corps))>:<chemin>

en ECDSA/SHA-256 avec la clé privée P-256, et on envoie la signature en base64.
Le *chemin* est celui de l'URL, hôte exclu. Les trois en-têtes doivent porter la
MÊME date que celle signée, à la seconde près, sinon Apple répond 401 sans dire
laquelle des trois est en cause.

La clé privée vit dans /root/.cloudkit/eckey.pem sur l'ovm, montée en lecture
seule dans le conteneur : le dossier du projet est publié sur un dépôt GitHub
public, elle ne doit jamais s'en approcher.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional
from urllib import error as urlerror
from urllib import request as urlrequest

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

HOST = "https://api.apple-cloudkit.com"


class CloudKitError(RuntimeError):
    pass


class CloudKit:
    def __init__(self,
                 container: str,
                 key_id: str,
                 private_key_path: str,
                 environment: str = "production",
                 database: str = "public",
                 timeout: int = 30):
        self.container = container
        self.key_id = key_id
        self.environment = environment
        self.database = database
        self.timeout = timeout
        with open(private_key_path, "rb") as fh:
            self._key = serialization.load_pem_private_key(fh.read(), password=None)
        if not isinstance(self._key, ec.EllipticCurvePrivateKey):
            raise CloudKitError("la clé privée n'est pas une clé EC P-256")

    # -- signature ---------------------------------------------------------

    def _sign(self, path: str, body: bytes) -> Dict[str, str]:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        digest = base64.b64encode(hashlib.sha256(body).digest()).decode()
        message = f"{date}:{digest}:{path}".encode()
        signature = self._key.sign(message, ec.ECDSA(hashes.SHA256()))
        return {
            "X-Apple-CloudKit-Request-KeyID": self.key_id,
            "X-Apple-CloudKit-Request-ISO8601Date": date,
            "X-Apple-CloudKit-Request-SignatureV1": base64.b64encode(signature).decode(),
            "Content-Type": "application/json",
        }

    # -- transport ---------------------------------------------------------

    def _post(self, operation: str, payload: Dict[str, Any], retries: int = 3) -> Dict[str, Any]:
        path = (f"/database/1/{self.container}/{self.environment}"
                f"/{self.database}/{operation}")
        body = json.dumps(payload, separators=(",", ":")).encode()

        last_error: Optional[Exception] = None
        for attempt in range(retries):
            req = urlrequest.Request(HOST + path, data=body,
                                     headers=self._sign(path, body), method="POST")
            try:
                with urlrequest.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode())
            except urlerror.HTTPError as exc:
                detail = exc.read().decode(errors="replace")[:500]
                # 503 et 429 valent une nouvelle tentative ; 401 et 400 non — réessayer
                # une signature refusée ou un corps invalide ne fait que perdre du temps.
                if exc.code in (429, 500, 502, 503) and attempt < retries - 1:
                    last_error = CloudKitError(f"HTTP {exc.code}: {detail}")
                    time.sleep(2 ** attempt)
                    continue
                raise CloudKitError(f"HTTP {exc.code} sur {path} : {detail}") from exc
            except urlerror.URLError as exc:
                last_error = exc
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise CloudKitError(f"réseau : {exc}") from exc
        raise CloudKitError(str(last_error))

    # -- lecture -----------------------------------------------------------

    def query(self,
              record_type: str,
              filters: Optional[List[Dict[str, Any]]] = None,
              results_limit: int = 200,
              desired_keys: Optional[List[str]] = None) -> Iterator[Dict[str, Any]]:
        """Parcourt TOUS les enregistrements, marqueur de continuation compris.

        Oublier le marqueur est l'erreur qui a tronqué le classement à une page
        côté app ; ici on ne rend la main qu'une fois la dernière page lue.
        """
        marker: Optional[str] = None
        seen = 0
        while True:
            query: Dict[str, Any] = {"recordType": record_type}
            if filters:
                query["filterBy"] = filters
            payload: Dict[str, Any] = {"query": query, "resultsLimit": results_limit}
            if desired_keys:
                payload["desiredKeys"] = desired_keys
            if marker:
                payload["continuationMarker"] = marker

            data = self._post("records/query", payload)
            for record in data.get("records", []):
                seen += 1
                yield record

            marker = data.get("continuationMarker")
            if not marker:
                return

    def lookup(self, record_names: List[str], chunk: int = 190) -> Dict[str, Dict[str, Any]]:
        """Récupère des enregistrements par NOM, sans passer par une requête.

        Une `query` exige un index *queryable* sur le type interrogé, et seuls
        `Prediction` en a un ici : lister `UserProfile` ou `UserScoreV2` répond
        « Field 'recordName' is not marked queryable ». Le lookup n'a pas cette
        contrainte — et il ne rapatrie que les enregistrements dont on a besoin.

        Un nom absent n'est pas une erreur : il manque simplement du résultat.
        """
        found: Dict[str, Dict[str, Any]] = {}
        names = [n for n in dict.fromkeys(record_names) if n]
        for start in range(0, len(names), chunk):
            batch = names[start:start + chunk]
            data = self._post("records/lookup",
                              {"records": [{"recordName": n} for n in batch]})
            for rec in data.get("records", []):
                if "serverErrorCode" in rec:
                    continue
                found[rec.get("recordName", "")] = rec
        return found

    # -- écriture ----------------------------------------------------------

    @staticmethod
    def field(value: Any) -> Dict[str, Any]:
        return {"value": value}

    def save(self, records: List[Dict[str, Any]], chunk: int = 190) -> Dict[str, int]:
        """Crée ou remplace des enregistrements, par paquets.

        CloudKit plafonne une opération de modification à 200 enregistrements ;
        on reste en dessous. Chaque résultat est inspecté individuellement : une
        opération peut réussir globalement alors qu'un enregistrement a été
        refusé — c'est exactement ainsi que l'app a cru pendant une semaine
        écrire des scores qui n'existaient pas.
        """
        saved = 0
        failed = 0
        errors: List[str] = []

        for start in range(0, len(records), chunk):
            batch = records[start:start + chunk]
            payload = {
                "operations": [
                    {"operationType": "forceReplace", "record": r} for r in batch
                ]
            }
            data = self._post("records/modify", payload)
            for entry in data.get("records", []):
                if "serverErrorCode" in entry:
                    failed += 1
                    if len(errors) < 10:
                        errors.append(f"{entry.get('recordName')}: "
                                      f"{entry.get('serverErrorCode')} "
                                      f"{entry.get('reason', '')}".strip())
                else:
                    saved += 1

        return {"saved": saved, "failed": failed, "errors": errors}


def from_env() -> CloudKit:
    """Construit le client depuis l'environnement du conteneur."""
    container = os.environ.get("CLOUDKIT_CONTAINER", "iCloud.Planc")
    key_id = os.environ.get("CLOUDKIT_KEY_ID", "")
    key_path = os.environ.get("CLOUDKIT_KEY_PATH", "/root/.cloudkit/eckey.pem")
    env = os.environ.get("CLOUDKIT_ENV", "production")
    if not key_id:
        raise CloudKitError("CLOUDKIT_KEY_ID manquant")
    return CloudKit(container=container, key_id=key_id,
                    private_key_path=key_path, environment=env)
