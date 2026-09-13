from temporalio.activity import defn
import logging
from typing import Dict, Any
from datetime import timedelta

logger = logging.getLogger(__name__)

@defn
async def run_test_generation(
    output_file: str,
    base_dir: str,
    model: str,
    provider: str,
    display_goose_log: bool = True,
) -> Dict[str, Any]:
    """
    Activity 4 : Génère les tests et déplace le prompt si valides.

    Args:
        output_file: Fichier généré à tester
        base_dir: Répertoire racine
        model: Modèle LLM
        provider: Provider LLM

    Returns:
        Dict avec tests_passed, prompt_moved, etc.
    """
    from src.utils.goose_cli import run_goose_command

    logger.info("=== Activity 4: Test Generation ===")
    logger.info(f"Output File: {output_file}")

    # Exécution de Goose CLI
    result = await run_goose_command(
        recipe="04-test_generation.yaml",
        model=model,
        provider=provider,
        cwd=base_dir,
        timeout=timedelta(hours=3),
        log_streaming=display_goose_log,
    )

    # Vérification des résultats des tests
    tests_passed = result.get("output", {}).get("tests_passed", False)

    return {
        "success": True,
        "tests_passed": tests_passed,
        "output_file": output_file
    }
