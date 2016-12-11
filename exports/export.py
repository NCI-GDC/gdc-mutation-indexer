from config import BaseConfig
from builders import MAFBuilder


class GDCMutationExport(object):

    def __init__(self, config=BaseConfig):
        self.config = config
    
    def run_export(config):
        # Construct master MAF from all individual MAFs
        builder = MAFBuilder()
        df = builder.build()
