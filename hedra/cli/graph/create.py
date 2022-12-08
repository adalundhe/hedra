import inspect
from pathlib import Path
from hedra.projects.graphs.generation import (
   Generator
)


def create_graph(path: str):
    generator = Generator()

    serialized_imports = generator.generate_imports()
    serialized_stages = generator.generate_stages()

    with open(path, 'w') as generated_test:
        generated_test.write(f'{serialized_imports}\n\n')
        generated_test.write(f'{serialized_stages}\n')