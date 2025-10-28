# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "typed-argument-parser",
# ]
# ///

import dataclasses
import os
import pathlib
import subprocess
from collections.abc import Sequence
from typing import NamedTuple

import tap


class Script(NamedTuple):
    name: str
    extras: Sequence[str]
    entry_point: str


SCRIPTS = (
    Script(
        name="driver-ge",
        extras=("gene-expression",),
        entry_point="mutation_indexer.gene_expression.diver:main",
    ),
    Script(name="driver-viz", extras=("viz",), entry_point="mutation_indexer.viz.driver:main"),
)


@dataclasses.dataclass(frozen=True)
class Builder:
    output: pathlib.Path
    """The output directory of the resulting scripts."""

    def build(self, script: Script) -> None:
        venv = f"build/{script.name}"
        output = self.output / f"{script.name}.pyz"

        subprocess.run(
            ("uv", "pip", "install", f"--target={venv}", f".[{','.join(script.extras)}]")
        )
        subprocess.run(
            (
                "uv",
                "run",
                "-m",
                "zipapp",
                f"--output={output}",
                f"--main={script.entry_point}",
                "--compress",
                venv,
            )
        )


def main() -> None:
    builder = tap.tapify(Builder)
    os.environ["VIRTUAL_ENV"] = ".venv"

    os.makedirs(builder.output, exist_ok=True)
    os.makedirs("build", exist_ok=True)

    for script in SCRIPTS:
        builder.build(script)


if __name__ == "__main__":
    main()
