"""Configuration centralisée du logging pour l'outillage DevOrchestra.

Dans Temporal, le *workflow* (l'orchestrateur) et les *activities* (qui lancent le
subprocess goose) s'exécutent dans des workers/processus séparés. Leurs logs sont
agrégés par le serveur sans ordre garanti : il est donc difficile, à la lecture d'un
journal combiné, de savoir de quel flux provient chaque ligne.

Ce module ajoute un tag visible (`[WORKFLOW]` / `[ACTIVITY]`) à chaque enregistrement,
déduit du nom de son logger, afin de distinguer clairement les deux flux.
"""

import logging

# Format avec le tag de source inséré entre la date et le niveau.
_LOG_FORMAT = "%(asctime)s | %(stream_tag)-9s | %(levelname)-7s | %(name)s | %(message)s"
_DATE_FORMAT = "%H:%M:%S"


class StreamTagFilter(logging.Filter):
    """Attache l'attribut ``stream_tag`` à un record selon la source du logger."""

    @staticmethod
    def _source_name(logger_name: str) -> str:
        """Normalise le nom en retirant un préfixe ``src.`` éventuel."""
        return logger_name[len("src."):] if logger_name.startswith("src.") else logger_name

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003 - nom imposé par l'API logging
        source = self._source_name(getattr(record, "name", ""))
        if source == "workflow" or source.startswith("workflow."):
            record.stream_tag = "WORKFLOW"
        elif source.startswith("activities") or source.startswith("utils"):
            record.stream_tag = "ACTIVITY"
        else:
            record.stream_tag = "MAIN"
        return True


def _build_handler() -> logging.StreamHandler:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    handler.addFilter(StreamTagFilter())
    return handler


def configure_logging(level: int = logging.INFO) -> None:
    """Configure le logger racine avec un tag de source (WORKFLOW / ACTIVITY).

    Remplace proprement ``logging.basicConfig`` pour garantir un format unique,
    que le code soit exécuté côté workflow (main.py) ou côté déclenchement
    (trigger_pipeline.py).
    """
    root = logging.getLogger()
    root.setLevel(level)
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(_build_handler())
