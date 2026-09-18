# src/workflow.py
import logging
from typing import Dict, Any
from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

# Imports des activities au niveau module (obligatoire pour Temporal sandbox)
from src.config import GooseConfig as gc
from src.activities.feature_plan import run_feature_plan
from src.activities.feature_build import run_feature_build
from src.activities.feature_validate import run_feature_validate
from src.activities.review_analyse import run_review_analysis
from src.activities.review_fix import run_review_fix
from src.activities.test_generate import run_test_generation
from src.activities.doc_generate import run_doc_generation
from src.activities.file_operations import move_prompt_to_directory, check_prompt_state

logger = logging.getLogger(__name__)


@workflow.defn
class develop_feature_pipeline:
    """Workflow principal : Pipeline complet de développement de fonctionnalité.

    Ce pipeline est résumable : il détermine l'emplacement actuel du fichier prompt
    (dossiers to_do_feature_plan / to_do_feature_build / to_do_feature_validate) et n'exécute que les étapes
    non encore réalisées.

    Règles d'enchaînement :
      - ÉTAPE 1 (analyse et génération) : exécutée uniquement si le prompt est dans to_do_feature_plan / to_do_feature_build.
      - ÉTAPE 2 (correction) : conditionnelle, si des issues sont trouvées à l'étape 2.
      - ÉTAPE 3 (tests) : exécutée si le prompt n'est pas déjà validé (pas en 03).
      - ÉTAPE 4 (documentation) : exécutée si le prompt est dans 03-to_validate et qu'au moins une étape précédente a été réalisée.
    """

    async def state_prompt(self, base_dir, feature_name):
        return await workflow.execute_activity(
            check_prompt_state,
            args=[base_dir, feature_name],
            start_to_close_timeout=timedelta(minutes=5),
        )

    @workflow.run
    async def develop_feature_pipeline(
        self,
        feature_name: str,
        base_dir: str,
        model: str = gc.model,
        provider: str = gc.provider,
        recipes_dir: str = "",
        max_turns: str = "100",
        display_goose_log: bool = True,
    ):

        logger.info(f"=== Starting Pipeline for Feature: {feature_name} ===")

        # === Détermination de l'emplacement actuel du prompt ===
        # L'activity localise le fichier prompt dans 01/02/03-to_* afin de ne
        # réexécuter que les étapes non encore réalisées (pipeline résumable).
        logger.info("Détermination de l'emplacement du fichier prompt ")

        state = await self.state_prompt(base_dir, feature_name)
        if not state["exists"]:
            logger.error(f"Aucun fichier prompt trouvé pour '{feature_name}' dans les dossiers")
            return {
                "success": False,
                "feature_name": feature_name,
                "status": "failed",
                "error_message": f"Aucun fichier prompt trouvé pour '{feature_name}'",
            }

        logger.info(f"{state}")
        logger.info(f"Location : {state['location']}")

        # Au moins une étape précédente réalisée si le prompt est déjà dans un état plus avancé
        logger.info(f"Emplacement actuel du prompt: {state['location']} | ")

        # === Étape 1.1 : Génération de la fonctionnalité
        # (uniquement si le prompt est dans to_do_feature_plan) ===
        if state["location"] == "to_do_feature_plan":
            logger.info("=== Étape 1.1 : Génération de la fonctionnalité ===")
            plan_result = await workflow.execute_activity(
                run_feature_plan,
                args=[feature_name, base_dir, model, provider, recipes_dir, max_turns, display_goose_log],
                start_to_close_timeout=timedelta(hours=gc.plan_timeout),
                retry_policy=RetryPolicy(
                    maximum_attempts=gc.max_retries,
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=5),
                ),
            )

            # === Déplacement du prompt de to_do_feature_plan vers to_do_feature_build ===
            logger.info("=== Déplacement du prompt de to_do_feature_plan vers to_do_feature_build ===")
            move_result = await workflow.execute_activity(
                move_prompt_to_directory,
                args=[base_dir, feature_name, "to_do_feature_plan", "to_do_feature_build"],
                start_to_close_timeout=timedelta(minutes=5),
            )

            error_move = move_result.get("error", "pas de message d'erreur")
            if move_result["move_success"]:
                logger.info(f"Prompt file '{feature_name}.yml' déplacé de to_do_feature_plan vers to_do_feature_build.")

                state = await self.state_prompt(base_dir, feature_name)
                if not state["exists"]:
                    logger.error(f"Aucun fichier prompt trouvé pour '{feature_name}' dans les dossiers")
                    return {
                        "success": False,
                        "feature_name": feature_name,
                        "status": "failed",
                        "error_message": f"Aucun fichier prompt trouvé pour '{feature_name}'",
                    }
            else:
                logger.warning(f"Echec du déplacement avec l'erreur : {error_move}")
        else:
            logger.info("Étape 1.1: Le fichier prompt n'est pas dans le dossier to_do_feature_plan, génération déjà réalisée -> étape suivante")

        # === ÉTAPE 1.2 : Génération de la fonctionnalité
        # (uniquement si le prompt est dans to_do_feature_build) ===
        if state["location"] == "to_do_feature_build":
            logger.info("=== Étape 1.2 : Génération de la fonctionnalité ===")
            generate_result = await workflow.execute_activity(
                run_feature_build,
                args=[feature_name, base_dir, model, provider, recipes_dir, max_turns, display_goose_log],
                start_to_close_timeout=timedelta(hours=gc.build_timeout),
                retry_policy=RetryPolicy(
                    maximum_attempts=gc.max_retries,
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=5),
                ),
            )

            # === Déplacement du prompt de to_do_feature_build vers to_do_feature_validate ===
            logger.info("=== Déplacement du prompt de to_do_feature_build vers to_do_feature_validate ===")
            move_result = await workflow.execute_activity(
                move_prompt_to_directory,
                args=[base_dir, feature_name, "to_do_feature_build", "to_do_feature_validate"],
                start_to_close_timeout=timedelta(minutes=5),
            )

            error_move = move_result.get("error", "pas de message d'erreur")
            if move_result["move_success"]:
                logger.info(f"Prompt file '{feature_name}.yml' déplacé de to_do_feature_build vers to_do_feature_validate.")

                state = await self.state_prompt(base_dir, feature_name)
                if not state["exists"]:
                    logger.error(f"Aucun fichier prompt trouvé pour '{feature_name}' dans les dossiers")
                    return {
                        "success": False,
                        "feature_name": feature_name,
                        "status": "failed",
                        "error_message": f"Aucun fichier prompt trouvé pour '{feature_name}'",
                    }
            else:
                logger.warning(f"Echec du déplacement avec l'erreur : {error_move}")
        else:
            logger.info("Étape 1.2: Le fichier prompt n'est pas dans le dossier to_do_feature_build, génération déjà réalisée -> étape suivante.")

        # === ÉTAPE 1.3 : Génération de la fonctionnalité
        # (uniquement si le prompt est dans to_do_feature_validate) ===
        if state["location"] == "to_do_feature_validate":
            logger.info("=== Étape 1.3 : Génération de la fonctionnalité ===")
            validate_result = await workflow.execute_activity(
                run_feature_validate,
                args=[feature_name, base_dir, model, provider, recipes_dir, max_turns, display_goose_log],
                start_to_close_timeout=timedelta(hours=gc.validate_timeout),
                retry_policy=RetryPolicy(
                    maximum_attempts=gc.max_retries,
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(minutes=5),
                ),
            )

            # === Déplacement du prompt de to_do_feature_build vers to_do_review ===
            logger.info("=== Déplacement du prompt de to_do_feature_build vers to_do_review ===")
            move_result = await workflow.execute_activity(
                move_prompt_to_directory,
                args=[base_dir, feature_name, "to_do_feature_validate", "to_do_review"],
                start_to_close_timeout=timedelta(minutes=5),
            )

            error_move = move_result.get("error", "pas de message d'erreur")
            if move_result["move_success"]:
                logger.info(f"Prompt file '{feature_name}.yml' déplacé de to_do_feature_validate vers to_do_review.")

                state = await self.state_prompt(base_dir, feature_name)
                if not state["exists"]:
                    logger.error(f"Aucun fichier prompt trouvé pour '{feature_name}' dans les dossiers")
                    return {
                        "success": False,
                        "feature_name": feature_name,
                        "status": "failed",
                        "error_message": f"Aucun fichier prompt trouvé pour '{feature_name}'",
                    }
            else:
                logger.warning(f"Echec du déplacement avec l'erreur : {error_move}")
        else:
            logger.info("Étape 1.3: Le fichier prompt n'est pas dans le dossier to_do_feature_validate, génération déjà réalisée -> étape suivante.")

        # === ÉTAPE 2 : Analyse et correction de la branche courante
        # (uniquement si le prompt est dans to_do_review) ===
        if state["location"] == "to_do_review":
            logger.info("=== Étape 2.1 : Analyse de la branche courante ===")
            review_result = await workflow.execute_activity(
                run_review_analysis,
                args=[base_dir, model, provider, display_goose_log],
                start_to_close_timeout=timedelta(hours=gc.review_timeout),
                retry_policy=RetryPolicy(
                    maximum_attempts=gc.max_retries,
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                ),
            )

            logger.info("=== Étape 2.2: Correction de la branche courante ===")
            fix_result = await workflow.execute_activity(
                run_review_fix,
                args=[base_dir, model, provider, None, display_goose_log],
                start_to_close_timeout=gc.fix_timeout,
                retry_policy=RetryPolicy(
                    maximum_attempts=gc.max_retries,
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                ),
            )
            logger.info(f"Fixes Applied: {fix_result['fixes_applied']}")

            # === Déplacement du prompt de to_do_review vers to_do_test_generate ===
            logger.info("=== Déplacement du prompt de to_do_review vers to_do_test_generate ===")
            move_result = await workflow.execute_activity(
                move_prompt_to_directory,
                args=[base_dir, feature_name, "to_do_review", "to_do_test_generate"],
                start_to_close_timeout=timedelta(minutes=5),
            )

            error_move = move_result.get("error", "pas de message d'erreur")
            if move_result["move_success"]:
                logger.info(f"Prompt file '{feature_name}.yml' déplacé de to_do_review vers to_do_test_generate.")

                state = await self.state_prompt(base_dir, feature_name)
                if not state["exists"]:
                    logger.error(f"Aucun fichier prompt trouvé pour '{feature_name}' dans les dossiers")
                    return {
                        "success": False,
                        "feature_name": feature_name,
                        "status": "failed",
                        "error_message": f"Aucun fichier prompt trouvé pour '{feature_name}'",
                    }
            else:
                logger.warning(f"Echec du déplacement avec l'erreur : {error_move}")
        else:
            # Le prompt n'est pas en to_do_review : on saute l'analyse (étape 2) et la
            # correction (étape 3) pour passer directement à la génération des tests.
            logger.info("Étape 2 et 3 : Le prompt n'est pas dans le dossier to_do_review, exclusion de l'analyse -> étape 4.")
            fix_result = {"fixes_applied": 0}

        # === ÉTAPE 3 : Génération des tests ===
        # Le chemin du prompt courant sert de référence pour l'étape de test.
        if state["location"] == "to_do_test_generate":
            logger.info("=== Étape 3 : Génération des tests ===")
            test_result = await workflow.execute_activity(
                run_test_generation,
                args=[state["path"], base_dir, model, provider, display_goose_log],
                start_to_close_timeout=timedelta(hours=gc.test_timeout),
                retry_policy=RetryPolicy(
                    maximum_attempts=gc.max_retries,
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                ),
            )

            # On conserve donc la valeur calculée à l'étape d'analyse (review) afin de ne pas perdre l'information

            # === Déplacement du prompt de to_do_review vers to_do_doc_generate ===
            logger.info("=== Déplacement du prompt de to_do_test_generate vers to_do_doc_generate ===")
            move_result = await workflow.execute_activity(
                move_prompt_to_directory,
                args=[base_dir, feature_name, "to_do_test_generate", "to_do_merge"],
                start_to_close_timeout=timedelta(minutes=5),
            )

            error_move = move_result.get("error", "pas de message d'erreur")
            if move_result["move_success"]:
                logger.info(f"Prompt file '{feature_name}.yml' déplacé de to_do_test_generate vers to_do_doc_generate.")

                state = await self.state_prompt(base_dir, feature_name)
                if not state["exists"]:
                    logger.error(f"Aucun fichier prompt trouvé pour '{feature_name}' dans les dossiers")
                    return {
                        "success": False,
                        "feature_name": feature_name,
                        "status": "failed",
                        "error_message": f"Aucun fichier prompt trouvé pour '{feature_name}'",
                    }
            else:
                logger.warning(f"Echec du déplacement avec l'erreur : {error_move}")
        else:
            # Le prompt n'est pas en to_do_test_generate
            logger.info("Étape 4 : Le prompt n'est pas dans le dossier to_do_test_generate, exclusion des tests -> étape 5.")
            test_result = {"tests_applied": 0}

        # === ÉTAPE 4 : Génération de la documentation ===
        if state["location"] == "to_do_doc_generate":
            logger.info("=== Étape 4 : Génération de la documentation ===")
            doc_result = await workflow.execute_activity(
                run_doc_generation,
                args=[state["path"], base_dir, model, provider, display_goose_log],
                start_to_close_timeout=timedelta(hours=gc.doc_timeout),
                retry_policy=RetryPolicy(
                    maximum_attempts=gc.max_retries,
                    initial_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0,
                ),
            )

            # === Déplacement du prompt de to_do_review vers to_do_doc_generate ===
            logger.info("=== Déplacement du prompt de to_do_review vers to_do_doc_generate ===")
            move_result = await workflow.execute_activity(
                move_prompt_to_directory,
                args=[base_dir, feature_name, "to_do_doc_generate", "to_merge"],
                start_to_close_timeout=timedelta(minutes=5),
            )

            error_move = move_result.get("error", "pas de message d'erreur")
            if move_result["move_success"]:
                logger.info(f"Prompt file '{feature_name}.yml' déplacé de to_do_doc_generate vers to_merge.")
            else:
                logger.warning(f"Echec du déplacement avec l'erreur : {error_move}")

            return {
                "success": True,
                "feature_name": feature_name,
            }
        else:
            # Le prompt n'est pas en 03-to_validate : on saute les dernières (étapes 4 et 5) pour finaliser le processus.
            logger.info("Étape 5: Le prompt n'est pas dans le dossier to_do_doc_generate, fin du processus")

        logger.info("=== Pipeline Completed Successfully ===")
