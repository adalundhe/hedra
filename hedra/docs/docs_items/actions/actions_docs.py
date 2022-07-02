from hedra.parsing.actions_parser import ActionsParser


class ActionsDocs:


    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg


    def print_docs(self):

        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")
            docs_item = docs_items[1]

            item_type = None
            if len(docs_items) > 2:
                item_type = docs_items[2]

            if docs_item == "parsers":

                if item_type is not None:
                    parser = ActionsParser.parsers.get(item_type)
                    print(parser.about())

                else:
                    print(ActionsParser.about())