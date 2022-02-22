from hedra.execution.personas import PersonaManager


class PersonasDocs:


    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg


    def print_docs(self):

        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")

            docs_item = docs_items[1]

            if docs_item == "batches":

                batches_about_item = None

                if len(docs_items) > 2:
                    batches_about_item = docs_items[2]

                if batches_about_item:
                    print(PersonaManager.about_batches(batches_about_item=batches_about_item))

                else:
                    print(PersonaManager.about_batches())

            else:
                persona = PersonaManager.registered_personas.get(docs_item)
                print(persona.about())

        else:

            print(PersonaManager.about())