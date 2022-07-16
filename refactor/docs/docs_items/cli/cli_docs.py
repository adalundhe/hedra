class CliDocs:


    def __init__(self, docs_arg, config_help_string) -> None:
        self.docs_arg = docs_arg
        self.config_help_string = config_help_string


    def print_docs(self):
        print(self.config_help_string)
