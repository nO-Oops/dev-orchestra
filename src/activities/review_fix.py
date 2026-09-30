from temporalio.activity import defn
from src.config import gc

# src/activities/fix.py
import logging
from typing import Dict, Any
from datetime import timedelta

logger = logging.getLogger(__name__)

@defn
async def run_review_fix(
    feature_name: str,
    base_dir: str,
    model: str,
    provider: str,
    review_report: str,
    display_goose_log: bool = True,
) -> Dict[str, Any]:
    """
    Activity 3 : Corrige les erreurs identifiées dans la branche courante.

    Args:
        base_dir: Répertoire racine
        model: Modèle LLM
        provider: Provider LLM
        review_report: Rapport d'analyse précédent

    Returns:
        Dict avec le résultat de la correction
    """
    from src.utils.goose_cli import run_goose_command, build_goose_log_path

    logger.info("=== Activity 3: Review Fix ===")

    # Exécution de Goose CLI (pas de gestion de session goose : --name / --resume supprimés)
    log_file = build_goose_log_path(base_dir, f"{feature_name}_review_fix")
    result = await run_goose_command(
        recipe="02-2-review_fix.yaml",
        model=model,
        provider=provider,
        cwd=base_dir,
        timeout=timedelta(hours=gc.fix_timeout),
        log_streaming=display_goose_log,
        log_file=log_file,
    )

    return {
        "success": True,
        "fixes_applied": result.get("output", {}).get("fixes_applied", 0)
    }
