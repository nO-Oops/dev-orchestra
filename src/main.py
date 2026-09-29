# src/main.py
import asyncio
import logging
import signal
import dataclasses
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner
from src.workflow import develop_feature_pipeline
from src.logging_setup import configure_logging
from src.config import tc
from src.activities.feature_plan import run_feature_plan
from src.activities.feature_build import run_feature_build
from src.activities.feature_validate import run_feature_validate
from src.activities.review_analyse import run_review_analysis
from src.activities.review_fix import run_review_fix
from src.activities.test_generate import run_test_generation
from src.activities.doc_generate import run_doc_generation
from src.activities.file_operations import move_prompt_to_directory, check_prompt_state, git_commit_after_move, create_feature_branch

configure_logging()
logger = logging.getLogger(__name__)

async def main():
    # Connecter au serveur Temporal (hôte/port issus de l'environnement)
    client = await Client.connect(tc.dsn)

    # Définir les activités
    activities = [
        run_feature_plan,
        run_feature_build,
        run_feature_validate,
        run_review_analysis,
        run_review_fix,
        run_test_generation,
        run_doc_generation,
        move_prompt_to_directory,
        check_prompt_state,
        git_commit_after_move,
        create_feature_branch,
    ]

    # Runner workflow avec modules pass-through.
    # src.config (et sa dépendance dotenv) lisent le fichier .env et utilisent
    # pathlib au moment de l'import ; ces opérations sont interdites dans le
    # sandbox Temp dans lequel Temporal exécute les workflows. La configuration
    # étant fixe et déterministe, on autorise l'import de ces modules hors sandbox.
    workflow_runner = SandboxedWorkflowRunner()
    workflow_runner = dataclasses.replace(
        workflow_runner,
        restrictions=workflow_runner.restrictions.with_passthrough_modules(
            "src.config", "dotenv"
        ),
    )

    # Créer le worker
    worker = Worker(
        client,
        task_queue="goose-pipeline-queue",
        workflows=[develop_feature_pipeline],
        activities=activities,
        workflow_runner=workflow_runner,
    )

    # Gérer les signaux d'arrêt propre
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Received shutdown signal")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    logger.info("Starting Temporal Worker...")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
