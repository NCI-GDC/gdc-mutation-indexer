import abc
import asyncio
import contextlib
import datetime
import itertools
import os
import pathlib
import tempfile
import uuid
from collections.abc import Iterable, Iterator, Mapping
from importlib import resources
from os import path
from typing import Any, Generic, TypeVar

import elasticsearch
import halo
import marshmallow
import more_itertools
import toml
from gdcmodels import esutils

from mutation_indexer import configuration
from mutation_indexer.configuration import environment

TConfig = TypeVar("TConfig", bound=configuration.Configuration)
MUTATION_INDEXER_DIR = "/var/tungsten/services/mutation_indexer"


def _merge_dict(a: dict, b: dict) -> None:
    """
    Merges the dictionary values from b into a. Any dictionary values in b will be
    recursively merged into the value for a.

    Args:
        a: The target dictionary to be updated
        b: The dictionary containing the values to be merged into a
    """
    for key, value in b.items():
        if isinstance(value, dict):
            _merge_dict(a.setdefault(key, {}), value)

        else:
            a[key] = value


def _get_manifest_file(manifest_dir: str, build_id: uuid.UUID) -> str:
    """
    Returns:
        the path to the manifest file into which the build's configuration will be
        recorded.
    """
    return path.join(
        manifest_dir,
        f"{datetime.datetime.now().isoformat()}-{build_id}.toml",
    )


def _set_environment_variables(env: environment.Environment) -> None:
    """
    Sets the environmental variables needed to run spark submit.

    Args:
        env: the configured values for the environmental variables.
    """
    os.environ["JAVA_HOME"] = env.java_home
    os.environ["SPARK_HOME"] = env.spark_home
    os.environ["YARN_CONF_DIR"] = env.yarn_conf_dir


class Client(Generic[TConfig], abc.ABC):
    def __init__(
        self,
        schema: marshmallow.Schema,
        obfuscated_schema: marshmallow.Schema,
        driver_module: str,
    ) -> None:
        self._schema = schema
        self._obfuscated_schema = obfuscated_schema
        self._driver_module = driver_module

    def _write_manifest(self, config: TConfig) -> None:
        """
        Writes the configuration data into the manifest with all secret values obfuscated.

        Args:
            config: the configuration with which the build was run.
        """
        build = config.build
        file_name = _get_manifest_file(build.manifest_dir, build.build_id)
        data: dict = self._obfuscated_schema.dump(config)  # type: ignore

        os.makedirs(build.manifest_dir, exist_ok=True)

        with open(file_name, "w+") as f:
            toml.dump(data, f)

    def _load_config_data(
        self, user_config_file: pathlib.Path, final_config_file: str
    ) -> Mapping[str, Any]:
        """
        Loads the user provided configuration and updates it with any required default
        values.

        Args:
            user_config_file: the configuration file provided by the user
            final_config_file: the file into which the final configuration data will be
                persisted into.

        Returns:
            the final configuration data as a mapping.
        """
        default_config = toml.loads(
            resources.read_text(self._driver_module, "configuration.toml")
        )
        default_config["build"]["config_file"] = final_config_file
        user_config = toml.load(user_config_file)

        _merge_dict(default_config, user_config)

        return default_config

    @contextlib.contextmanager
    def _get_config(
        self,
        user_config_file: pathlib.Path,
    ) -> Iterator[TConfig]:
        """
        Loads the configuration data and persists the raw data into a temporary file which
        can be uploaded with the spark-submit command. The context manager returned insures
        that the temporary file is removed and that the data is obfuscated and stored in the
        manifest file.

        Args:
            user_config_file: The path to the user provided configuration file.

        Returns:
            A context manager which in turn provides the configuration object with which to
            run the application.
        """
        config = None

        try:
            with tempfile.TemporaryDirectory() as temp_directory:
                config_file = path.join(temp_directory, "configuration.toml")
                config_data = self._load_config_data(user_config_file, config_file)
                config: Any = self._schema.load(config_data)  # type: ignore

                with open(config_file, "w+") as f:
                    toml.dump(config_data, f)

                yield config

        finally:
            if config is not None:
                self._write_manifest(config)

    def _get_file_args(self, config: TConfig) -> Iterable[tuple[str, str]]:
        """
        Sets the spark-submit config values as well as jars params.

        Yields:
            a tuple of argument flag and value.
        """
        build = config.build
        files = ",".join(
            (
                f"{config.build.config_file}#configuration.toml",
                path.join(
                    MUTATION_INDEXER_DIR, "mutation-indexer.pex#mutation-indexer.pex"
                ),
            )
        )

        yield (
            "--conf",
            f"spark.yarn.dist.files={files}",
        )
        yield (
            "--jars",
            ",".join(
                path.join(build.jar_dir, jar) for jar in os.listdir(build.jar_dir)
            ),
        )

    async def _run_spark_command(self, config: TConfig) -> None:
        """
        Runs the spark-submit command which will spawn the spark application. The spark
        application will build the desired indices.

        Args:
            config: the configuration for the build.
        """
        config_arguments = config.spark.get_arguments()
        file_arguments = self._get_file_args(config)
        arguments = more_itertools.flatten(
            itertools.chain(config_arguments, file_arguments)
        )
        spark_home = os.getenv("SPARK_HOME", "")
        spark_command = path.join(spark_home, "bin/spark-submit")
        final_command = " ".join(
            more_itertools.value_chain(
                "sudo -E -u ubuntu",
                spark_command,
                arguments,
                path.join(MUTATION_INDEXER_DIR, "mutation_indexer.py"),
                self._driver_module,
            )
        )
        home_dir = os.environ.get("HOME", "")

        output_file = path.join(
            tempfile.gettempdir() or home_dir, "mutation-indexer.log"
        )
        error_file = path.join(
            tempfile.gettempdir() or home_dir, "mutation-indexer-error.log"
        )

        with open(output_file, "wb+") as out_f, open(error_file, "wb+") as error_f:
            process = await asyncio.create_subprocess_shell(
                final_command, stdout=out_f, stderr=error_f
            )

            await process.wait()

    def _force_merge_indices(self, config: TConfig) -> None:
        """
        Performs a force merge on the indices that have been created.

        Args:
            config: The configuration with which the build was run.
        """
        with elasticsearch.Elasticsearch(
            config.elasticsearch.connection.nodes.split(","),
            use_ssl=config.elasticsearch.connection.use_ssl,
            verify_certs=config.elasticsearch.connection.verify_certs,
            http_auth=(
                config.elasticsearch.connection.user,
                config.elasticsearch.connection.password,
            ),
        ) as es_client:
            indices = [
                index
                for index in config.elasticsearch.write.indices.values()
                if es_client.indices.exists(index=index)
            ]

            esutils.force_merge_elasticsearch_indices(es_client, indices)

    async def run(self, config_path: pathlib.Path) -> None:
        with self._get_config(config_path) as config:
            print(f"RUNNING BUILD: {config.build.build_id}")
            _set_environment_variables(config.environment)

            with halo.Halo(spinner="pong") as spinner:
                try:
                    spinner.text = "Running spark-submit"
                    await self._run_spark_command(config)
                    spinner.text = "Merging indices"
                    self._force_merge_indices(config)
                except:
                    spinner.fail("Process Failed")
                    raise
                else:
                    spinner.succeed("Indices built")
