"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @ https://wiki.uchicago.edu/display/CDIS/Mutation+Indexer+Configuration
"""
import dataclasses


@dataclasses.dataclass(frozen=True)
class Environment:
    """
    Configuration values for setting environmental valriables.
    """

    java_home: str
    spark_home: str
    yarn_conf_dir: str
