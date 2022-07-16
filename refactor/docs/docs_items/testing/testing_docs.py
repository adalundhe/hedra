from hedra.core.pipelines.stages import Execute
from hedra.test.config import Config
from hedra.test.hooks import (
    setup,
    action,
    teardown,
    use
)


class TestingDocs:

    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg

        self.hooks_types = {
            "setup": setup,
            "action": action,
            "teardown": teardown,
            "use": use
        }

    def print_docs(self):

        if ":" in self.docs_arg:
            docs_items = self.docs_arg.split(":")

            item_type = docs_items[1]
            hooks_type = None

            if len(docs_items) > 2:
                hooks_type = docs_items[2]

            if item_type == "action-set":
                print(Execute.about())

            elif item_type == "test": 
                print(Config.about())

            elif item_type == "hooks":

                if hooks_type is None:
             
                    print(Execute.about_hooks())

                else:

                    hook = self.hooks_types.get(hooks_type)
                    print(hook.__doc__)

        else:

            testing_docs_string = '''
        Testing

        Hedra's Testing package allows you to write performance tests as Python code, with full freedom to use
        any Python packages or Hedra's own built-in engines and action types. To use this package, you must
        write test code as a Python class that inherits from the ActionSet class contained within the Testing
        package. You must also then wrap class methods with any of the provided "hooks" (Python decorators denoting
        that a class method is a valid action).


        For more information on how to use the ActionSet class, run the command:

            hedra --about testing:action-set

        For more information on how to implement code configuration of tests via the Test class, run the
        command:

            hedra --about testing:test

        For more information on hooks, run the command:

            hedra --about testing:hooks


        Related Topics:

            - engines
            - personas
            - runners

            '''
            
            print(testing_docs_string)