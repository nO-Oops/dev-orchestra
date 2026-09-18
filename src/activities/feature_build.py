"""
Activity 1 : Génère une fonctionnalité à partir d'un prompt.
"""

import logging
from datetime import timedelta
from typing import Any, Dict

from temporalio import activity
from src.config import GooseConfig as gc

logger = logging.getLogger(__name__)


@activity.defn
async def run_feature_build(
    feature_name: str,
    base_dir: str,
    model: str,
    provider: str,
    recipes_dir: str = "",
    max_turns: str = "100",
    display_goose_log: bool = True,
) -> Dict[str, Any]:
    """
    Génère une fonctionnalité à partir d'un prompt en utilisant Goose CLI.

    Args:
        feature_name: Nom de la fonctionnalité
        base_dir: Répertoire racine de l'application
        model: Modèle LLM
        provider: Provider LLM

    Returns:
        Dict avec les résultats de la génération
    """
    from src.utils.goose_cli import run_goose_command

    logger.info("=== Activity 1: Feature Generation ===")
    logger.info(f"Feature: {feature_name}")
    logger.info(f"Base Dir: {base_dir}")

    # Vérification des paramètres requis
    if not feature_name:
        raise ValueError("feature_name est requis")
    if not base_dir:
        raise ValueError("base_dir est requis")

    try:
        # Convertir max_turns en int pour Goose CLI
        max_turns_int = int(max_turns) if max_turns else 100

        # Convertir recipes_dir vide en None pour Goose CLI
        recipes_dir_value = recipes_dir if recipes_dir else None

        # Exécution de Goose CLI
        logger.info("Exécution de Goose CLI...")
        result = await run_goose_command(
            recipe="01-2-feature_build.yaml",
            model=model,
            provider=provider,
            interactive=False,
            cwd=base_dir,
            timeout=timedelta(hours=gc.build_timeout),
            recipes_dir=recipes_dir_value,
            max_turns=max_turns_int,
            log_streaming=display_goose_log,
        )

        # Extraction des résultats
        output = result.get("output", {})

        logger.info(f"Goose CLI a retourné: {list(output.keys())}")

        return {
            "success": True,
            "feature_name": feature_name,
            "output": output,
        }

    except Exception as e:
        import traceback
        # Capture le traceback complet AVANT de lever l'exception
        full_traceback = traceback.format_exc()
        logger.error(f"Erreur lors de la génération de la feature:\n{full_traceback}")
        # Utiliser ApplicationError pour une meilleure sérialisation Temporal
        from temporalio.exceptions import ApplicationError
        raise ApplicationError(
            message=f"Échec de la génération de la feature: {str(e)}",
            non_retryable=False,
        ) from e
