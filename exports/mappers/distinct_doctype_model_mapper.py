from model_mapper import ModelMapper


class DistinctDocTypeModelMapper(ModelMapper):
    """
    The base class assumes that index name == doc_type.
    This subclass allows you to explicitly specify the document type.
    """

    def __init__(self, index, doc_type):
        ModelMapper.__init__(self, index)
        self.doc_type = doc_type
