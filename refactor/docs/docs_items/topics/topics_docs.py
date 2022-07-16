
class TopicsDocs:

    def __init__(self, docs_arg) -> None:
        self.docs_arg = docs_arg

        self.topics = [
            "pipelines",
            "personas",
            "optimizers",
            "engines",
            "runners",
            "results",
            "testing",
            "cli"
        ]

    
    def print_docs(self):
        docs_topics = '\n\t'.join([f'- {topic}' for topic in self.topics])

        docs_string = f'''
        Hedra Interactive Docs

        Welcome to Hedra interactive docs! Using the --about argument, you may pass a string
        to learn more about the following supported topics:

        {docs_topics}

        For example:

            hedra --about pipelines

        will tell you about Hedra's Pipelines.

        To learn more about sub-topics (if available) within a topic, simple use a colon to 
        seperate the topic and subtopic you wish to learn about. For example:

            hedra --about pipelines:warmup

        will tell you about the Warmup Pipeline stage. Feel free to explore these interactive 
        docs to learn more about Hedra's architecture, options, and tips/tricks for successful 
        performance testing!

        Good luck!

        '''

        print(docs_string)
