import inspect
import sys
import importlib
import ntpath
import importlib
from collections import defaultdict
from typing import Dict, Any, Optional
from pathlib import Path


class Generator:

    def __init__(self, generator_types: Dict[str, Any], known_module_paths: Dict[str, str]={}) -> None:
        self.generator_types = generator_types
        self.known_module_paths = known_module_paths
        self.global_imports = {}
        self.local_imports = defaultdict(list)
        self.locals = []

        self.serialized_global_imports = []
        self.serialized_local_imports = []
        self.serialized_locals = []

    def gather_required_items(self, generator_type: str) -> Dict[str, Any]:
        generator_item = self.generator_types.get(generator_type)
        path = inspect.getfile(generator_item)

        package_dir = Path(path).resolve().parent
        package_dir_path = str(package_dir)
        package_dir_module = package_dir_path.split('/')[-1]

        package = ntpath.basename(path)
        package_slug = package.split('.')[0]
        spec = importlib.util.spec_from_file_location(f'{package_dir_module}.{package_slug}', path)

        if path not in sys.path:
            sys.path.append(str(package_dir.parent))

        module = importlib.util.module_from_spec(spec)
        sys.modules[module.__name__] = module

        spec.loader.exec_module(module)

        local_file_items = [item for item in dir(module) if not item.startswith("__")]
        modules = {
            member_name: member for member_name, member in inspect.getmembers(module) if member_name in local_file_items
        }

        for module_name, module in modules.items():

            try:
                same_source_file = inspect.getfile(module) == inspect.getfile(generator_item)

                if same_source_file and module_name != generator_item.__name__:
                    self.locals.append(module)

            except TypeError:
                pass

        self.locals.append(generator_item)

        return modules

    def collect_imports(self, generator_type: Optional[str], modules: Dict[str, Any]):

        generator_item = self.generator_types.get(generator_type)
        generator_item_name = None

        if generator_item:
            generator_item_name = generator_item.__name__

        for module_name, module in modules.items():
            if inspect.ismodule(module):
                self.global_imports[module.__name__] = module.__name__

            elif module_name != generator_item_name and module not in self.locals:

                module_path = self.known_module_paths.get(module_name, module.__module__)
                minimum_viable_import_path = module_path.split('.')


                run_minimum_path_search = True
                while run_minimum_path_search:
                    minimum_viable_import_path.pop()
                    
                    if len(minimum_viable_import_path) > 0:
                        candidate_module_path = '.'.join(minimum_viable_import_path)

                        source_module = importlib.import_module(candidate_module_path)
                        source_module_members = {
                            source_module_name: source_module for source_module_name, source_module in inspect.getmembers(source_module)
                        }

                        if module_name in source_module_members:
                            module_path = candidate_module_path
                    else:
                        run_minimum_path_search = False
                    
                self.local_imports[module_path].append(module_name)

    def serialize_items(self):

        self.serialized_global_imports = [
            f'import {global_import}' for global_import in self.global_imports
        ]

        for local_import_module, local_import_items in self.local_imports.items():
            serialized_import_items = '\n\t'.join([
                f'{local_import_item},' for local_import_item in local_import_items
            ])

            self.serialized_local_imports.append(
                f'from {local_import_module} import (\n\t{serialized_import_items}\n)'
            )

        self.serialized_locals = [
            inspect.getsource(local) for local in self.locals
        ]

    
