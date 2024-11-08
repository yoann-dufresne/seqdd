
import importlib
import logging
import pkgutil
from collections.abc import KeysView
from types import ModuleType


class SourceManager:

    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger) -> None:
        src_modules = SourceManager.list_and_load_sources()
        self.sources = dict()
        for src_module in src_modules:
            if not hasattr(src_module, 'naming'):
                continue
            src_init = getattr(src_module, src_module.naming['classname'])
            self.sources[src_module.naming['key']] = src_init(tmpdir, bindir, logger)

    def keys(self) -> KeysView[str]:
        return self.sources.keys()
    
    def get(self, key:str) -> str | None:
        return self.sources.get(key, None)

    # --- Submodules ---
    @staticmethod
    def source_keys() -> list[str]:
        src_modules = SourceManager.list_and_load_sources()
        return [mod.naming['key'] for mod in src_modules if hasattr(mod, 'naming')]

    @staticmethod
    def list_and_load_sources() -> list[ModuleType]:
        # Charger le module principal
        src_module_name = 'seqdd.register.data_type'
        module = importlib.import_module(src_module_name)
        
        # Lister les sous-modules
        submodules = [name for _, name, _ in pkgutil.iter_modules(module.__path__)]
        
        # Charger dynamiquement chaque sous-module et les ajouter à une liste
        loaded_sources = [importlib.import_module(f"{src_module_name}.{submodule}") for submodule in submodules]
        
        return loaded_sources
