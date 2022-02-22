from hedra.execution import pipelines
from hedra.execution.pipelines import Pipeline



class PipelineDocs:


    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg


    def print_docs(self):

        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")

            pipeline_stage = docs_items[1]

            stage = Pipeline.stages.get(pipeline_stage)

            print(stage.about())

        else:

            print(Pipeline.about())