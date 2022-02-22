from .runner_docs_shell import RunnerDocsShell


class RunnerDocs:


    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg

        self.runners = RunnerDocsShell()


    def print_docs(self):

        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")

            runner_type = docs_items[1]

            doc_string = self.runners.about(runner_type)

            print(doc_string)

        else:

            runner_types = '\n\t'.join([f'- {runner}' for runner in self.runners.runner_types.keys()])

            about_runners_string = f'''
        Runners

        key-arguments:

        --runner-mode <runner_type>

        --actions-filepath <path_to_actions_JSON_file>

        --code-filepath <path_to_actions_Python_file> (action-set engine only)

        --config-filepath <path_to_json_file_containing_cli_args>

        --log-level <string_log_level> (debug, info, warning, and error are supported)

        Runners are responsible for provisioning hardware resources, controlling distributed execution, or otherwise
        initiating, running, and exiting test execution as a whole. This includes thread management, worker job size
        provisioning, worker status update managment, etc.

        Currently registered runner types include:

        {runner_types}


        For more information about a given runner type, run the command:

            hedra --about runners:<runner_type>


        Related Topics:

            - results


            '''

            print(about_runners_string)