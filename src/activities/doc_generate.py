from temporalio.activity import defn
from src.config import GooseConfig as gc

# src/activities/doc_gen.py
import logging
from typing import Dict, Any
from datetime import timedelta

logger = logging.getLogger(__name__)

@defn
async def run_doc_generation(
    output_file: str,
    base_dir: str,
    model: str,
    provider: str,
    display_goose_log: bool = True,
) -> Dict[str, Any]:
    """
    Activity 5 : Génère la documentation liée à la fonctionnalité.

    Args:
        output_file: Fichier de la fonctionnalité
        base_dir: Répertoire racine
        model: Modèle LLM
        provider: Provider LLM

    Returns:
        Dict avec doc_generated, doc_path, etc.
    """
    from src.utils.goose_cli import run_goose_command

    logger.info("=== Activity 5: Documentation Generation ===")
    logger.info(f"Output File: {output_file}")

    # Exécution de Goose CLI
    result = await run_goose_command(
        recipe="05-doc_generation.yaml",
        model=model,
        provider=provider,
        cwd=base_dir,
        timeout=timedelta(hours=gc.doc_timeout),
        log_streaming=display_goose_log,
    )

    return {
        "success": True,
        "doc_generated": True,
        "output_file": output_file,
    }
