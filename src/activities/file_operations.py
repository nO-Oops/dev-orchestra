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


@defn
async def create_feature_branch(
    base_dir: str,
    feature_task_name: str,
) -> Dict[str, Any]:
    """
    Activity : Crée une branche git pour une fonctionnalité.

    Le nom de branche est généré selon les règles suivantes :
      - Tout en minuscules
      - Remplacer les espaces par "-"
      - Remplacer les caractères spéciaux par "-"
      - Remplacer les caractères accentués par le caractère sans accent

    Exemple : "Ajout de Filtres de Recherche !" -> "feature/ajout-de-filtres-de-recherche"

    Args:
        base_dir: Répertoire racine du projet (contenant le repo git)
        feature_task_name: Nom de la fonctionnalité

    Returns:
        Dict avec branch_created, branch_name et success
    """
    import subprocess
    import unicodedata
    import re

    # 1. Normaliser les caractères accentués (décomposition en base + diacritique)
    normalized = unicodedata.normalize('NFD', feature_task_name)
    # 2. Supprimer les diacritiques (caractères de catégorie 'Mn' = Mark, Nonspacing)
    branch_name = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    # 3. Tout en minuscules
    branch_name = branch_name.lower()
    # 4. Remplacer les caractères non alphanumériques par "-"
    branch_name = re.sub(r'[^a-z0-9]+', '-', branch_name)
    # 5. Nettoyer les "-" multiples et les "-" en début/fin
    branch_name = re.sub(r'-+', '-', branch_name).strip('-')

    full_branch_name = f"feature/{branch_name}"

    try:
        # Vérifier si la branche existe déjà
        branch_check = subprocess.run(
            ["git", "branch", "--list", full_branch_name],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if branch_check.stdout.strip():
            logger.info(f"Branche '{full_branch_name}' existe déjà, pas de création")
            return {"branch_created": False, "branch_name": full_branch_name, "success": True}

        # Créer la branche
        checkout_result = subprocess.run(
            ["git", "checkout", "-b", full_branch_name],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if checkout_result.returncode != 0:
            logger.error(f"Failed to create branch '{full_branch_name}': {checkout_result.stderr}")
            return {"branch_created": False, "branch_name": full_branch_name, "success": False, "error": checkout_result.stderr}

        logger.info(f"Branche '{full_branch_name}' créée")
        return {"branch_created": True, "branch_name": full_branch_name, "success": True}
    except Exception as e:
        logger.error(f"Failed to create feature branch: {e}")
        return {"branch_created": False, "branch_name": full_branch_name, "success": False, "error": str(e)}


@defn
async def git_commit_after_move(
    base_dir: str,
    feature_name: str,
    source_folder: str,
    target_folder: str,
) -> Dict[str, Any]:
    """
    Activity : Effectue un commit git après un déplacement de fichier prompt.

    Cette activity est appelée uniquement lorsque le déplacement du fichier prompt
    a réussi. Elle ajoute les changements et crée un commit avec un message descriptif.

    Les opérations de subprocess (git) sont interdites à l'intérieur d'un workflow
    Temporal (sandbox, déterminisme). Elles sont donc déléguées à cette activity
    qui s'exécute en dehors du sandbox.

    Args:
        base_dir: Répertoire racine du projet (contenant le repo git)
        feature_name: Nom de la fonctionnalité
        source_folder: Dossier source du déplacement
        target_folder: Dossier cible du déplacement

    Returns:
        Dict avec commit_success, committed et message de commit
    """
    import subprocess

    try:
        # Vérifier s'il y a des changements à committer
        status_result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if not status_result.stdout.strip():
            logger.info("Aucun changement à committer, skip du commit")
            return {"commit_success": True, "committed": False, "message": "No changes to commit"}

        # Ajouter les changements et committer
        subprocess.run(
            ["git", "add", "-A"],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )

        commit_message = (
            f"chore: move prompt '{feature_name}.yaml' from {source_folder} to {target_folder}"
        )

        subprocess.run(
            ["git", "commit", "-m", commit_message],
            cwd=base_dir,
            capture_output=True,
            text=True,
            timeout=60,
        )

        logger.info(f"Commit créé : {commit_message}")
        return {"commit_success": True, "committed": True, "message": commit_message}
    except Exception as e:
        logger.error(f"Failed to create git commit: {e}")
        return {"commit_success": False, "committed": False, "error": str(e)}


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
