import re
import asyncio
# src/utils/goose_cli.py
import json
import logging
from typing import Optional, Dict, Any
from pathlib import Path
from datetime import timedelta

logger = logging.getLogger(__name__)

class GooseCLIError(Exception):
    """Erreur spécifique à Goose CLI"""
    def __init__(self, message: str = "", non_retryable: bool = False):
        self.non_retryable = non_retryable
        super().__init__(message)

async def run_goose_command(
    recipe: str,
    model: str,
    provider: str = "llm-local",
    interactive: bool = False,
    cwd: Optional[str] = None,
    timeout: timedelta = timedelta(hours=12),
    extra_args: Optional[list] = None,
    first_run: bool = False,
    log_streaming: bool = True,
    recipes_dir: Optional[str] = None,
    # Budget de tours par défaut : les recipes multi-étapes (doc/test/review,
    # ~9-10 étapes à ~16 tours/étape) en consomment ~150-160. 100 était trop
    # bas et faisait s'arrêter Goose en milieu de workflow (code 0, sans
    # indicateur de complétion). 250 laisse une marge suffisante.
    max_turns: int = 250,
) -> Dict[str, Any]:
    """
    Exécute une commande Goose CLI avec gestion du session-id et des erreurs.

    Args:
        recipe: Nom du recipe YAML (ex: "01-feature_generate.yaml")
        model: Modèle LLM à utiliser
        provider: Provider LLM
        interactive: Mode interactif (pour la première étape)
        cwd: Répertoire de travail (application cible)
        timeout: Timeout d'exécution
        extra_args: Arguments supplémentaires
        log_streaming: Logger les sorties en temps réel (True) ou attendre la fin (False)
        recipes_dir: Chemin vers le répertoire des recipes. Si None, utilise "./.goose/recipes/"

    Returns:
        Dict contenant les résultats de l'exécution
    """
    logger.info(f"recipe: {recipe}")

    # Construire le chemin vers la recipe
    if recipes_dir:
        recipe_path = f"{recipes_dir}/{recipe}"
    else:
        # Par défaut, les recipes vivent dans le repo outillage (DevOrchestra),
        # c'est-à-dire là où se trouve ce code (src/utils/goose_cli.py -> repo root).
        # On résout un chemin ABSOLU afin que goose puisse lire la recipe quand bien
        # même son répertoire de travail (cwd = base_dir, le projet cible) est un autre
        # dossier. Cela permet à goose d'exécuter dans le projet cible tout en lisant
        # les recipes depuis le répertoire outillage.
        _tooling_root = str(Path(__file__).resolve().parents[2])  # src/utils -> src -> racine du repo
        recipes_dir = f"{_tooling_root}/.goose/recipes"
        recipe_path = f"{recipes_dir}/{recipe}"

    cmd = [
        "goose", "run",
        "--recipe", recipe_path,
        "--provider", provider,
        "--model", model,
        "--max-turns", str(max_turns),
    ]

    # Note : la gestion de session (--name / --resume) est volontairement supprimée.
    # Goose crée et gère ses propres sessions automatiquement à chaque exécution.
    # L'usage de --resume exige qu'une session existe déjà, ce qui casse la première
    # exécution ("No session found"). On laisse donc goose gérer les sessions lui-même.

    if interactive:
        cmd.append("--interactive")

    if extra_args:
        cmd.extend(extra_args)

    logger.info(f"Exécution Goose CLI : {' '.join(cmd)}")
    logger.info(f"CWD: {cwd or '.'}")

    process = None
    stdout_task: Optional[asyncio.Task] = None
    stderr_task: Optional[asyncio.Task] = None

    try:
        # Create a prompt from the recipe instruction if not provided
        prompt = f"Execute recipe: {recipe}"

        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=1024*1024
        )

        # Collecter les sorties en streaming si demandé
        stdout_chunks = []
        stderr_chunks = []

        async def stream_output(stream, chunks, prefix):
            """Logger les sorties en temps réel"""
            while True:
                line = await stream.readline()
                if not line:
                    break
                decoded = line.decode('utf-8', errors='replace').strip()
                if decoded:
                    chunks.append(decoded)
                    if log_streaming:
                        logger.info(f"{prefix}: {decoded}")

        # Lancer les tâches de streaming
        stdout_task = asyncio.create_task(stream_output(process.stdout, stdout_chunks, "STDOUT"))
        stderr_task = asyncio.create_task(stream_output(process.stderr, stderr_chunks, "STDERR"))

        # Envoyer le prompt et attendre la fin
        if process.stdin is not None:
            process.stdin.write(prompt.encode('utf-8'))
            await process.stdin.drain()
            process.stdin.close()

        # Attendre que le processus se termine
        await asyncio.wait_for(process.wait(), timeout=timeout.total_seconds())

        # Attendre que les tâches de streaming se terminent
        await asyncio.gather(stdout_task, stderr_task)

        stdout = '\n'.join(stdout_chunks)
        stderr = '\n'.join(stderr_chunks)

        # Vérifier le code de retour
        if process.returncode != 0:
            error_msg = f"Goose CLI failed with exit code {process.returncode}\nSTDERR: {stderr}"
            logger.error(error_msg)
            raise GooseCLIError(error_msg)

        # Vérifier les erreurs critiques dans stdout ET stderr.
        # Ces phrases signalent un blocage / un prérequis non satisfait (arrêt
        # légitime de Goose), à distinguer d'un crash en milieu de workflow.
        # La détection est bilingue (FR + EN) car la recipe et les réponses de
        # Goose sont en français.
        combined_output = f"{stdout}\n{stderr}".lower()
        critical_errors = [
            # --- Anglais ---
            "the token in keyring is invalid",
            "authentication failed",
            "authentication error",
            "invalid credentials",
            "token expired",
            "i cannot proceed",
            "cannot proceed until",
            "please:",
            "git working tree",
            "re-authenticate",
            # --- Français ---
            "arbre de travail sale",
            "arbre de travail non propre",
            "ne peux pas poursuivre",
            "ne peut pas poursuivre",
            "branch feature serait créée",
            "arrêter immédiatement",
            "prérequis non satisfait",
            "prérequis manquant",
        ]
        blocking_phrase = next((c for c in critical_errors if c in combined_output), None)
        if blocking_phrase:
            error_msg = (
                f"Goose CLI bloqué : prérequis non satisfait (phrase détectée : '{blocking_phrase}')\n"
                f"OUTPUT: {stdout}\nSTDERR: {stderr}"
            )
            logger.error(error_msg)
            raise GooseCLIError(
                f"Prérequis non satisfaits : Goose a arrêté le workflow à l'étape de "
                f"validation (probable arbre de travail sale). "
                f"Nettoyez le statut git du projet cible ('{cwd}') avant de relancer.",
                non_retryable=True,
            )

        # Vérifier que Goose a produit du contenu significatif
        combined_lower = f"{stdout}\n{stderr}".lower()
        if not combined_lower.strip():
            error_msg = (
                "Goose CLI exited with code 0 but produced no output.\n"
            )
            logger.error(error_msg)
            raise GooseCLIError(error_msg)

        # Vérifier que Goose a terminé toutes les étapes du recipe
        # Indicateurs de complétion complète (dernière étape du recipe)
        completion_indicators = [
            "pushed to",
            "branch pushed",
            "commit and push",
            "feature complete",
            "all tasks completed",
            "workflow complete",
            "pr created",
            "tâche terminée",
            "workflow terminé",
        ]
        completed = any(indicator in combined_lower for indicator in completion_indicators)

        if not completed:
            # Détecter si Goose s'est arrêté en milieu de workflow (exécution incomplète)
            error_msg = (
                f"Goose CLI exited with code 0 but did NOT complete the full workflow.\n"
                f"Missing completion indicators: {completion_indicators}\n"
                f"Output length: {len(stdout)} chars\n"
                f"Last 1000 chars: {stdout[-1000:] if stdout else 'empty'}\n"
            )
            logger.error(error_msg)
            raise GooseCLIError(
                f"Workflow incomplet : Goose s'est arrêté avant la fin. {error_msg}",
                non_retryable=True,
            )

        return {
            "success": True,
            "returncode": 0,
            "output": {
                "raw_output": stdout,
            },
            "stdout": stdout,
            "stderr": stderr
        }

    except asyncio.TimeoutError:
        error_msg = f"Goose CLI timed out after {timeout}"
        logger.error(error_msg)
        # En cas de timeout, on tue le processus
        if process is not None and process.returncode is None:
            process.terminate()
        raise GooseCLIError(error_msg)
    except FileNotFoundError:
        error_msg = "goose CLI not found. Install it with: go install github.com/block/goose@latest"
        logger.error(error_msg)
        raise GooseCLIError(error_msg)
    finally:
        # Nettoyage garanti des tâches de streaming
        if stdout_task is not None and not stdout_task.done():
            stdout_task.cancel()
        if stderr_task is not None and not stderr_task.done():
            stderr_task.cancel()
