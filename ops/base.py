"""OpsToolBase — base class per i tool Ops (azioni on-demand, non schedulate).

Simmetrico a CheckBase (checks/base.py) ma per azioni che richiedono input
dall'utente e vengono lanciate on-demand da CLI o TUI, non da cron.
"""

from __future__ import print_function

from checks.base import check_requires


class OpsParam(object):
    """Dichiarazione di un parametro richiesto da un tool Ops.

    Un solo posto dichiara nome/help/tipo/obbligatorietà — sia il CLI
    (argparse) sia la futura TUI generano la loro interfaccia di input
    a partire da questa lista, senza duplicare la dichiarazione.
    """

    def __init__(self, name, help, required=True, type=str):
        # type: (str, str, bool, type) -> None
        self.name = name
        self.help = help
        self.required = required
        self.type = type


class OpsToolBase(object):
    """
    Base class per i tool Ops.

    Sottoclassi devono:
    - Definire `name` (id univoco, usato da CLI/registry/TUI)
    - Definire `params` come lista di OpsParam
    - Definire `requires` come lista di liste (stesso OR-of-AND-list di CheckBase)
    - Definire `is_write = True` se il tool modifica stato esterno al cluster
      (nessun tool oggi lo fa — riservato per usi futuri, es. un tool che
      aggiorna un inventory e fa push su un repo esterno)
    - Implementare `run(**kwargs)` che restituisce un CheckResult
    """
    name = ""
    description = ""
    params = []      # type: list
    requires = []     # type: list
    is_write = False

    def __init__(self, config, caps):
        # type: (dict, dict) -> None
        self.config = config
        self.caps = caps

    def can_run(self):
        # type: () -> bool
        return check_requires(self.requires, self.caps)

    def run(self, **kwargs):
        # type: (...) -> object
        raise NotImplementedError(
            "Implement run() in {}".format(self.__class__.__name__)
        )
