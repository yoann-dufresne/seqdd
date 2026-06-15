"""Helpers to check that the external command-line tools SeqDD relies on are installed."""

import shutil
from typing import Iterable


def missing_binaries(names: Iterable[str]) -> list[str]:
    """
    Return the subset of the given binary names that cannot be found on the PATH.

    :param names: An iterable of executable names (e.g. 'curl', 'wget').
    :return: The sorted list of names for which no executable was found.
    """
    return [name for name in sorted(set(names)) if shutil.which(name) is None]


def required_binaries_for(register) -> set[str]:
    """
    Collect the external binaries required to download every non-empty data type of a register.

    :param register: A Register whose data containers expose a ``source.required_binaries`` set.
    :return: The union of required binaries across the non-empty subregisters.
    """
    needed: set[str] = set()
    for container in register.data_containers.values():
        if len(container) > 0:
            needed |= set(getattr(container.source, "required_binaries", ()))
    return needed
