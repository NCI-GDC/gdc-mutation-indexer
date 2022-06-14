import dataclasses


@dataclasses.dataclass(frozen=True)
class Environment:
    java_home: str
    spark_home: str
    yarn_conf_dir: str
