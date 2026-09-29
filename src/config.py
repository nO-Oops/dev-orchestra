"""Configuration centralisée de l'outillage DevOrchestra.

Source de vérité unique : le fichier ``.env`` (à la racine du projet) et, en cas
de priorité supérieure, les variables d'environnement réelles (``os.environ``).

L'application ne possède **aucune valeur par défaut codée en dur** : chaque
réglage doit être présent dans ``.env`` (ou dans l'environnement). Si une valeur
manque, une ``ConfigurationError`` est levée au démarrage (fail-fast) afin de
forcer la complétion du fichier de configuration.

Hiérarchie de priorité (de la plus forte à la plus faible) :

  1. Variables d'environnement réelles (``os.environ``).
  2. Fichier ``.env`` situé à la racine du repo (parent de ``src/``).

Le fichier ``.env`` est chargé une seule fois au premier import de ce module
(moyennant la présence de ``python-dotenv``).

Deux singletons sont exposés :

  * ``gc``  (``GooseConfig``)      : réglages Goose CLI / pipeline.
  * ``tc``  (``TemporalConfig``)   : réglages de connexion au serveur Temporal.

Ils sont construits une seule fois à l'import, ce qui les rend stables pour le
replay deterministe des workflows Temporal.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from datetime import timedelta

# python-dotenv est une dépendance requise : sans elle, le fichier .env ne peut
# pas être chargé et l'application refuse de démarrer.
try:
    from dotenv import load_dotenv

    _DOTENV_AVAILABLE = True
except Exception:  # pragma: no cover - cas où dotenv est absent
    _DOTENV_AVAILABLE = False


class ConfigurationError(RuntimeError):
    """Levée lorsqu'une valeur de configuration est absente de l'environnement.

    Il n'existe aucune valeur par défaut : toute la configuration doit provenir
    du fichier ``.env`` (ou des variables d'environnement).
    """


def _find_env_file() -> Path:
    """Localise et retourne le chemin du fichier ``.env`` du projet.

    Levune ``ConfigurationError`` s'il est introuvable à la racine du repo.
    """
    here = Path(__file__).resolve()
    env_path = here.parents[1] / ".env"
    if env_path.exists():
        return env_path
    raise ConfigurationError(
        "Le fichier .env est introuvable à la racine du projet "
        f"(attendu dans : {env_path}). Créez-le à partir du modèle fourni."
    )


def _load_dotenv() -> None:
    """Charge ``.env`` dans ``os.environ`` (les vars existantes sont préservées)."""
    if not _DOTENV_AVAILABLE:
        raise ConfigurationError(
            "La dépendance 'python-dotenv' est requise pour charger le fichier .env."
        )
    load_dotenv(str(_find_env_file()), override=False)


# Chargement au premier import : activités et workflow lisent ``gc`` / ``tc``.
_load_dotenv()


def _require(key: str) -> str:
    """Lit une string obligatoire dans l'environnement (défaut interdit)."""
    value = os.environ.get(key)
    if value is None or value == "":
        raise ConfigurationError(
            f"La variable d'environnement '{key}' est requise "
            f"(définissez-la dans le fichier .env)."
        )
    return value


def _require_int(key: str) -> int:
    """Lit un entier obligatoire (défaut interdit)."""
    raw = _require(key)
    try:
        return int(raw)
    except ValueError:
        raise ConfigurationError(
            f"La variable d'environnement '{key}' doit être un entier "
            f"(reçu : {raw!r})."
        )


def _require_float(key: str) -> float:
    """Lit un flottant obligatoire (défaut interdit)."""
    raw = _require(key)
    try:
        return float(raw)
    except ValueError:
        raise ConfigurationError(
            f"La variable d'environnement '{key}' doit être un nombre "
            f"(reçu : {raw!r})."
        )


@dataclass
class GooseConfig:
    """Configuration pour les appels Goose CLI et le pipeline.

    Construit exclusivement à partir de l'environnement via ``from_env()`` :
    aucun champ n'a de valeur par défaut.
    """

    # Modèle / provider
    model: str
    provider: str

    # Timeouts (durées max d'exécution, en HEURES)
    plan_timeout: int
    build_timeout: int
    validate_timeout: int
    review_timeout: int
    fix_timeout: int
    test_timeout: int
    doc_timeout: int

    # Retry policy
    max_retries: int
    retry_initial_interval: timedelta
    retry_backoff_coefficient: float

    # Chemins
    base_dir: str
    prompts_dir: str
    goose_recipes_dir: str

    @classmethod
    def from_env(cls) -> "GooseConfig":
        """Construit la config à partir de l'environnement (valeurs obligatoires)."""
        return cls(
            model=_require("GOOSE_MODEL"),
            provider=_require("GOOSE_PROVIDER"),
            plan_timeout=_require_int("PLAN_TIMEOUT"),
            build_timeout=_require_int("BUILD_TIMEOUT"),
            validate_timeout=_require_int("VALIDATE_TIMEOUT"),
            review_timeout=_require_int("REVIEW_TIMEOUT"),
            fix_timeout=_require_int("FIX_TIMEOUT"),
            test_timeout=_require_int("TEST_TIMEOUT"),
            doc_timeout=_require_int("DOC_TIMEOUT"),
            max_retries=_require_int("MAX_RETRIES"),
            retry_initial_interval=timedelta(
                seconds=_require_int("RETRY_INITIAL_INTERVAL_SECONDS")
            ),
            retry_backoff_coefficient=_require_float("RETRY_BACKOFF_COEFFICIENT"),
            base_dir=_require("BASE_DIR"),
            prompts_dir=_require("PROMPTS_DIR"),
            goose_recipes_dir=_require("GOOSE_RECIPES_DIR"),
        )


@dataclass
class TemporalConfig:
    """Configuration de connexion au serveur Temporal (valeurs obligatoires)."""

    host: str
    port: int
    task_queue: str

    @property
    def dsn(self) -> str:
        """Retourne l'adresse ``host:port`` pour ``Client.connect``."""
        return f"{self.host}:{self.port}"

    @classmethod
    def from_env(cls) -> "TemporalConfig":
        """Construit la config à partir de l'environnement (valeurs obligatoires)."""
        return cls(
            host=_require("TEMPORAL_HOST"),
            port=_require_int("TEMPORAL_PORT"),
            task_queue=_require("TEMPORAL_TASK_QUEUE"),
        )


# Singletons construits une seule fois à l'import (stables pour le replay).
gc = GooseConfig.from_env()
tc = TemporalConfig.from_env()


@dataclass
class FeaturePrompt:
    """Représentation d'un prompt à traiter"""
    file_path: str           # Chemin relatif vers le fichier YAML
    feature_name: str        # Nom de la fonctionnalité (ex: "01-031_local-library-and-history")
    status: str = "pending"  # pending | in_progress | completed | failed


@dataclass
class PipelineResult:
    """Résultat du pipeline"""
    feature_name: str
    review_report: Optional[str] = None
    tests_passed: bool = False
    doc_generated: bool = False
    status: str = "success"  # success | failed | aborted
    error_message: Optional[str] = None
