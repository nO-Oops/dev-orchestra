#!/usr/bin/env python3
"""
Déclenche un workflow Temporal pour chaque prompt trouvé dans les dossiers de prompts.

L'endroit où se trouve chaque prompt (dossiers to_do_feature_plan / to_do_feature_build / to_do_feature_validate)
est déterminé par le workflow lui-même via l'activity check_prompt_state. Le dossier
n'est donc plus passé en paramètre.

Usage:
    python trigger_pipeline.py --project /chemin/vers/app
    python trigger_pipeline.py -p my-project -m Qwen3.6-35B-A3B-8bit -c 5
    python trigger_pipeline.py -p my-project --dry-run
    python trigger_pipeline.py -p my-project --no-display-goose-log
"""

import argparse
import asyncio
import logging
import uuid
from datetime import datetime
from pathlib import Path

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from src.logging_setup import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

# Configuration par défaut
DEFAULT_MODEL = "Qwen3.6-35B-A3B-6bit"
DEFAULT_PROVIDER = "llm-local"
DEFAULT_TEMPORAL_HOST = "localhost"
DEFAULT_TEMPORAL_PORT = 7233
DEFAULT_TASK_QUEUE = "goose-pipeline-queue"
DEFAULT_MAX_CONCURRENCY = 1

# Dossiers de prompts actifs parcourus dans l'ordre de priorité.
PROMPT_SUBFOLDERS = (
    "to_do_feature_plan",
    "to_do_feature_build",
    "to_do_feature_validate",
    "to_do_review",
    "to_do_review_fix",
    "to_do_test_generate",
    "to_do_doc_generate")

async def start_single_workflow(
    client: Client,
    feature_name: str,
    base_dir: str,
    model: str,
    provider: str,
    recipes_dir: str | None,
    max_turns: int,
    display_goose_log: bool,
    semaphore: asyncio.Semaphore,
) -> str | None:
    """Démarre un workflow pour un prompt unique avec contrôle de concurrence."""
    async with semaphore:
        workflow_id = f"feature-{feature_name}-{uuid.uuid4().hex[:8]}"
        try:
            args = [feature_name, base_dir, model, provider, recipes_dir or "", str(max_turns), display_goose_log]
            handle = await client.start_workflow(
                "develop_feature_pipeline",
                args=args,
                id=workflow_id,
                task_queue=DEFAULT_TASK_QUEUE,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
            )
            logger.info(f"✅ Workflow lancé  | ID: {handle.id} | Feature: {feature_name}")
            return handle.id
        except WorkflowAlreadyStartedError:
            logger.warning(f"⏳ Workflow déjà en cours pour {feature_name}, skip.")
            return None
        except Exception as e:
            logger.error(f"❌ Erreur lancement workflow {feature_name}: {e}")
            return None

async def main():
    parser = argparse.ArgumentParser(
        description="Déclenche les workflows Temporal pour tous les prompts d'un projet."
    )
    parser.add_argument("--project", "-p", required=True, help="Répertoire racine du projet (base_dir)")
    parser.add_argument("--model", "-m", default=DEFAULT_MODEL, help="Modèle LLM à utiliser")
    parser.add_argument("--provider", "-pr", default=DEFAULT_PROVIDER, help="Provider LLM")
    parser.add_argument("--temporal-host", default=DEFAULT_TEMPORAL_HOST, help="Hôte serveur Temporal")
    parser.add_argument("--temporal-port", type=int, default=DEFAULT_TEMPORAL_PORT, help="Port serveur Temporal")
    parser.add_argument("--task-queue", "-tq", default=DEFAULT_TASK_QUEUE, help="Task Queue Temporal")
    parser.add_argument("--max-concurrency", "-c", type=int, default=DEFAULT_MAX_CONCURRENCY, help="Nombre max de workflows simultanés")
    parser.add_argument("--dry-run", action="store_true", help="Liste les prompts sans lancer les workflows")
    parser.add_argument("--recipes-dir", "-rd", default=None, help="Chemin vers le répertoire des recipes (ex: /path/to/DevOrchestra/.goose/recipes)")
    parser.add_argument("--max-turns", "-mt", type=int, default=300, help="Nombre maximum d'actions (tournées) Goose CLI")
    parser.add_argument("--session-id", "-si", default=None, help="Session ID à transmettre aux activités (généré automatiquement si absent)")
    parser.add_argument(
        "--display-goose-log",
        dest="display_goose_log",
        action="store_true",
        default=True,
        help="Afficher les logs/sorties de Goose en temps réel (désactivable avec --no-display-goose-log)",
    )
    parser.add_argument(
        "--no-display-goose-log",
        dest="display_goose_log",
        action="store_false",
        help="Ne pas afficher les logs/sorties de Goose en temps réel",
    )
    args = parser.parse_args()

    # Validation du répertoire projet
    base_dir = Path(args.project).resolve()
    if not base_dir.exists():
        logger.error(f"❌ Répertoire projet introuvable: {base_dir}")
        return 1

    # Scan des dossiers de prompts actifs. Le workflow localise lui-même le prompt
    # (via check_prompt_state), aucun dossier n'est donc passé en paramètre.
    discovered = {}  # feature_name -> Path (premier dossier trouvé dans l'ordre de priorité)
    for subfolder in PROMPT_SUBFOLDERS:
        folder = base_dir / ".goose" / "prompts" / subfolder
        if not folder.exists():
            continue
        for p in list(folder.glob("*.yml")) + list(folder.glob("*.yaml")):
            discovered.setdefault(p.stem, p)

    prompt_files = sorted(discovered.values(), key=lambda x: x.stem)
    if not prompt_files:
        logger.warning(
            "⚠️ Aucun fichier .yml/.yaml trouvé dans les dossiers de prompts actifs "
            "(.goose/prompts/to_do_feature_plan, to_do_feature_build, to_do_feature_validate)"
        )
        return 0

    logger.info(f"🔍 {len(prompt_files)} prompt(s) détecté(s) dans les dossiers de prompts actifs")
    for p in prompt_files:
        logger.info(f"   • {p.relative_to(base_dir)}")

    if args.dry_run:
        logger.info("🏁 Mode dry-run activé. Aucun workflow lancé.")
        return 0

    # Connexion Temporal
    logger.info(f"🔌 Connexion à Temporal: {args.temporal_host}:{args.temporal_port}")
    try:
        client = await Client.connect(
            f"{args.temporal_host}:{args.temporal_port}",
        )
    except Exception as e:
        logger.error(f"❌ Impossible de connecter à Temporal: {e}")
        return 1

    logger.info(f"✅ Connecté. Task Queue: {args.task_queue}")

    # Lancement des workflows avec contrôle de concurrence
    semaphore = asyncio.Semaphore(args.max_concurrency)

    # Calculer le chemin vers les recipes si non fourni
    recipes_dir = args.recipes_dir
    if not recipes_dir:
        # Par défaut, utiliser les recipes du projet DevOrchestra (parent du projet cible)
        recipes_dir = f"{base_dir.parent}/DevOrchestra/.goose/recipes"

    for prompt_file in prompt_files:
        feature_name = prompt_file.stem
        await start_single_workflow(
            client=client,
            feature_name=feature_name,
            base_dir=str(base_dir),
            model=args.model,
            provider=args.provider,
            recipes_dir=recipes_dir,
            max_turns=args.max_turns,
            display_goose_log=args.display_goose_log,
            semaphore=semaphore,
        )

    logger.info(f"📊 Résumé: {len(prompt_files)} prompt(s) scanné(s) → workflows déclenchés")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
