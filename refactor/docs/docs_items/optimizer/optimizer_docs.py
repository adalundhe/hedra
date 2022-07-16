from hedra.core.optimizers import Optimizer

class OptimizerDocs:


    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg


    def print_docs(self):

        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")

            optimizer_type = docs_items[1]

            optimizer = Optimizer.optimizer_types.get(optimizer_type)

            print(optimizer.about())

        else:

            print(Optimizer.about())