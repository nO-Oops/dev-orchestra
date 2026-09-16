# src/activities/file_operations.py
import logging
from typing import Dict, Any

from temporalio import activity
from temporalio.activity import defn

logger = logging.getLogger(__name__)


@defn
async def move_prompt_to_directory(
    base_dir: str,
    feature_name: str,
    source_folder: str = "to_do_feature_plan",
    target_folder: str = "to_do_feature_build",
) -> Dict[str, Any]:
    """
    Activity : Déplace un fichier prompt entre dossiers de prompts.

    Les opérations filesystem (pathlib.Path.exists, shutil.move) sont interdites
    à l'intérieur d'un workflow Temporal (sandbox, déterminisme). Elles sont donc
    déléguées à cette activity qui s'exécute en dehors du sandbox.

    Args:
        base_dir: Répertoire racine du projet
        feature_name: Nom de la fonctionnalité (utilisé pour le nom du fichier .yaml)
        source_folder: Dossier source (défaut: to_do_feature_plan)
        target_folder: Dossier cible (défaut: to_do_feature_build)

    Returns:
        Dict avec move_success et les chemins concernés
    """
    import shutil
    from pathlib import Path

    source = Path(f"{base_dir}/.goose/prompts/{source_folder}/{feature_name}.yaml")
    target = Path(f"{base_dir}/.goose/prompts/{target_folder}")

    if not source.exists():
        logger.error(f"Source file not found: {source}")
        return {"move_success": False, "source": str(source), "dest": None}

    target.mkdir(parents=True, exist_ok=True)
    dest = target / source.name

    try:
        logger.info(f"Moved {source} -> {dest}")
        shutil.move(str(source), str(dest))
        return {"move_success": True, "source": str(source), "dest": str(dest)}
    except Exception as e:
        logger.error(f"Failed to move file: {e}")
        return {"move_success": False, "source": str(source), "dest": str(dest), "error": str(e)}


@activity.defn
async def check_prompt_state(
    base_dir: str,
    feature_name: str,
) -> Dict[str, Any]:
    """
    Activity : Détermine où se trouve le fichier prompt d'une fonctionnalité.

    Explore les dossiers to_do_feature_plan, to_do_feature_build, to_do_feature_validate et retourne le
    premier où le fichier {feature_name}.yml est présent (avec son chemin absolu).

    Les opérations filesystem (pathlib.Path.exists) sont interdites dans un workflow
    Temporal (sandbox, déterminisme). Elles sont donc déléguées à cette activity.

    Cette activity rend le pipeline résumable : le workflow peut ainsi déterminer
    quelle est la prochaine étape à exécuter sans réexécuter celles déjà réalisées.

    Args:
        base_dir: Répertoire racine du projet
        feature_name: Nom de la fonctionnalité (utilisé pour le nom du fichier .yml)

    Returns:
        Dict avec:
          - exists: bool (True si le prompt a été trouvé)
          - location: "to_do_feature_plan" | "to_do_feature_build" | "to_do_feature_validate" ... | None
          - path: chemin absolu vers le fichier (ou None)
    """
    from pathlib import Path

    folders = (
        "to_do_feature_plan",
        "to_do_feature_build",
        "to_do_feature_validate",
        "to_do_review",
        "to_do_review_fix",
        "to_do_test_generate",
        "to_do_doc_generate")
    for folder in folders:
        candidate = Path(f"{base_dir}/.goose/prompts/{folder}/{feature_name}.yaml")
        if candidate.exists():
            logger.info(f"Prompt '{feature_name}' trouvé dans {folder}")
            return {"exists": True, "location": folder, "path": str(candidate)}

    logger.info(f"Prompt '{feature_name}' introuvable dans {folders}")
    return {"exists": False, "location": None, "path": None}
