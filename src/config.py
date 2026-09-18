from dataclasses import dataclass, field
from typing import Optional
from datetime import timedelta

@dataclass
class GooseConfig:
    """Configuration pour les appels Goose CLI"""
    # Modèle par défaut
    model: str = "Qwen3.6-35B-A3B-8bit"
    provider: str = "llm-network"

    # Timeouts (durées max d'exécution)
    plan_timeout: int = 4
    build_timeout: int = 4
    validate_timeout: int = 4
    review_timeout: int = 4
    fix_timeout: int = 4
    test_timeout: int = 5
    doc_timeout: int = 4

    # Retry policy
    max_retries: int = 3
    retry_initial_interval: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    retry_backoff_coefficient: float = 2.0

    # Chemins (à adapter selon votre structure)
    base_dir: str = "/path/to/your/app"
    prompts_dir: str = "prompts"
    goose_recipes_dir: str = ".goose/recipes"


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
