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
import os
import uuid
from datetime import datetime
from pathlib import Path

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from src.logging_setup import configure_logging
from src.config import gc, tc, _require_int

configure_logging()
logger = logging.getLogger(__name__)

# Valeurs par défaut UNIFIÉES : la source de vérité unique est src/config.py
# (singletons gc / tc), elles-mêmes surchargeables via le fichier .env.
# Le modèle/ provider du déclencheur ne diffèrent plus de ceux du pipeline.
DEFAULT_MODEL = gc.model
DEFAULT_PROVIDER = gc.provider
DEFAULT_TEMPORAL_HOST = tc.host
DEFAULT_TEMPORAL_PORT = tc.port
DEFAULT_TASK_QUEUE = tc.task_queue
# Paramètres de lancement : valeurs par défaut issues de .env, surchargeables
# par les arguments CLI. Les chemins de projet/prompts restent dynamiques.
DEFAULT_MAX_CONCURRENCY = _require_int("MAX_CONCURRENCY")
DEFAULT_MAX_TURNS = _require_int("MAX_TURNS")
DEFAULT_RECIPES_DIR = os.environ.get("GOOSE_RECIPES_DIR") or None

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
    lock_trigger_path: str,
) -> object | None:
    """Démarre un workflow pour un prompt unique avec contrôle de concurrence."""
    async with semaphore:
        workflow_id = f"feature-{feature_name}-{uuid.uuid4().hex[:8]}"
        try:
            # Le verrou est global au déclenchement : il ne doit pas être transmis
            # aux workflows individuels, qui peuvent être rejoués ou tourner en parallèle.
            args = [feature_name, base_dir, model, provider, recipes_dir or "", str(max_turns), display_goose_log]
            handle = await client.start_workflow(
                "develop_feature_pipeline",
                args=args,
                id=workflow_id,
                task_queue=DEFAULT_TASK_QUEUE,
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
            )
            logger.info(f"✅ Workflow lancé  | ID: {handle.id} | Feature: {feature_name}")
            return handle
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
    parser.add_argument("--recipes-dir", "-rd", default=DEFAULT_RECIPES_DIR, help="Chemin vers le répertoire des recipes (défaut : GOOSE_RECIPES_DIR ou chemin calculé)")
    parser.add_argument("--max-turns", "-mt", type=int, default=DEFAULT_MAX_TURNS, help="Nombre maximum d'actions (tournées) Goose CLI")
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

    # Chemin explicite du verrou : le worker Temporal peut avoir un CWD différent
    # de celui du processus qui déclenche le pipeline.
    lock_trigger_path = str(Path.cwd().resolve().parent / "lock_trigger")

    # Lancement des workflows avec contrôle de concurrence
    semaphore = asyncio.Semaphore(args.max_concurrency)

    # Calculer le chemin vers les recipes si non fourni
    recipes_dir = args.recipes_dir
    if not recipes_dir:
        # Par défaut, utiliser les recipes du projet DevOrchestra (parent du projet cible)
        recipes_dir = f"{base_dir.parent}/DevOrchestra/.goose/recipes"

    workflow_handles = []
    failed_workflows = []

    for prompt_file in prompt_files:
        feature_name = prompt_file.stem
        handle = await start_single_workflow(
            client=client,
            feature_name=feature_name,
            base_dir=str(base_dir),
            model=args.model,
            provider=args.provider,
            recipes_dir=recipes_dir,
            max_turns=args.max_turns,
            display_goose_log=args.display_goose_log,
            semaphore=semaphore,
            lock_trigger_path=lock_trigger_path,
        )
        if handle is not None:
            workflow_handles.append(handle)

    logger.info(
        "⏳ Attente de la fin définitive de %s workflow(s) (retries Temporal inclus)...",
        len(workflow_handles),
    )
    # handle.result() ne se termine qu'à l'état terminal du workflow : les
    # retries d'activités configurés dans le workflow sont donc épuisés.
    # Si le déclencheur est annulé/interrompu pendant cette attente, l'exception
    # remonte et le verrou est volontairement conservé : les workflows Temporal
    # peuvent continuer même si ce processus local s'arrête.
    results = await asyncio.gather(
        *(handle.result() for handle in workflow_handles),
        return_exceptions=True,
    )
    for handle, result in zip(workflow_handles, results):
        if isinstance(result, BaseException):
            failed_workflows.append(handle.id)
            logger.error("❌ Workflow terminé en échec | ID: %s | %s", handle.id, result)
        else:
            logger.info("🏁 Workflow terminé | ID: %s", handle.id)

    logger.info(
        "📊 Résumé: %s prompt(s) scanné(s), %s workflow(s) terminé(s), %s échec(s)",
        len(prompt_files), len(workflow_handles), len(failed_workflows),
    )

    # Le verrou n'est supprimé qu'après le retour normal de l'attente de TOUS
    # les workflows. En cas d'interruption/annulation, cette ligne n'est pas atteinte.
    lock_path = Path(lock_trigger_path)
    try:
        lock_path.unlink(missing_ok=True)
        logger.info("🧹 Fichier lock_trigger supprimé après la fin de tous les workflows : %s", lock_path)
    except OSError:
        logger.exception("❌ Impossible de supprimer le fichier lock_trigger : %s", lock_path)

    return 1 if failed_workflows else 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
