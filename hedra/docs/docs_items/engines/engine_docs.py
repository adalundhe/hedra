from hedra.core.engines import Engine


class EngineDocs:


    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg


    def print_docs(self):

        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")

            engine_type = docs_items[1]
            engine = Engine.registered_engines.get(engine_type)

            engine_docs_item = None
            if len(docs_items) > 2:
                engine_docs_item = docs_items[2]

            if engine_docs_item is None:
                print(engine.about())

            else:

                if engine_type == 'playwright':

                    if engine_docs_item == 'librarian':
                        print(engine.about_librarian())

                    elif engine_docs_item == 'library':
                        print(engine.about_library())

                    elif engine_docs_item == 'list':
                        print(engine.about_registered_commands())

                    else:
                        print(engine.about_command(engine_docs_item))


        else:

            print(Engine.about())