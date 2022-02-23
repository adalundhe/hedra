class Documents:

    def __init__(self, documents):
        self.documents = documents
        if documents:
            self.count = len(documents)
        else:
            self.count = 0

    def single(self) -> dict:
        return self.documents.pop()

    def all(self) -> list:
        return self.documents