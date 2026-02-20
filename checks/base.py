"""CheckBase — base class per tutti i check di HadoopScope."""

from __future__ import print_function


class CheckResult(object):
    OK       = "OK"
    WARNING  = "WARNING"
    CRITICAL = "CRITICAL"
    UNKNOWN  = "UNKNOWN"
    SKIPPED  = "SKIPPED"

    def __init__(self, name, status, message, details=None):
        # type: (str, str, str, dict) -> None
        self.name    = name
        self.status  = status
        self.message = message
        self.details = details or {}

    def __repr__(self):
        return "CheckResult({}, {}, {})".format(self.name, self.status, self.message)


class CheckBase(object):
    """
    Base class per i check.

    Sottoclassi devono:
    - Definire `requires` come lista di liste: OR logico di AND-list
      es. requires = [["ansible"], ["docker"]]  →  ansible OR docker
          requires = [["ansible", "kinit"]]      →  ansible AND kinit
          requires = []                           →  sempre eseguibile
    - Definire `fallback` come altra classe Check (opzionale)
    - Implementare il metodo `run()` che restituisce un CheckResult
    """
    requires = []    # type: list
    fallback = None  # type: type

    def __init__(self, config, caps):
        # type: (dict, dict) -> None
        self.config = config
        self.caps   = caps

    def can_run(self):
        # type: () -> bool
        """
        Verifica se almeno una delle require-list è soddisfatta
        (OR logico tra le liste, AND logico dentro ogni lista).
        """
        if not self.requires:
            return True
        for req_list in self.requires:
            if all(self.caps.get(r, False) for r in req_list):
                return True
        return False

    def run(self):
        # type: () -> CheckResult
        raise NotImplementedError(
            "Implement run() in {}".format(self.__class__.__name__)
        )
