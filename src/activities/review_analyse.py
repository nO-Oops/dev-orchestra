from temporalio.activity import defn
# src/activities/review.py
import logging
from typing import Dict, Any
from datetime import timedelta

logger = logging.getLogger(__name__)

@defn
async def run_review_analysis(
    base_dir: str,
    model: str,
    provider: str,
    display_goose_log: bool = True,
) -> Dict[str, Any]:
    """
    Activity 2 : Analyse la branche courante et création d'un rapport.

    Args:
        base_dir: Répertoire racine
        model: Modèle LLM
        provider: Provider LLM

    Returns:
        Dict avec review_report, issues_found, etc.
    """
    from src.utils.goose_cli import run_goose_command

    logger.info("=== Activity 2: Review Analysis ===")

    logger.info(f"base_dir : {base_dir}")


    # Exécution de Goose CLI
    result = await run_goose_command(
        recipe="02-1-review_analysis.yaml",
        model=model,
        provider=provider,
        cwd=base_dir,
        interactive=False,
        timeout=timedelta(hours=3),
        first_run=False,
        log_streaming=display_goose_log,
    )

    # Extraction du rapport
    review_report = result.get("output", {}).get("review_report") or result.get("stdout")
    issues_found = result.get("output", {}).get("issues_count", 0)

    return {
        "success": True,
        "review_report": review_report,
        "issues_found": issues_found,
    }
