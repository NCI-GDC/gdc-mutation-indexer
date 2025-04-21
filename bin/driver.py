"""A wrapper script for calling the desired driver module."""

import runpy

import tap

from mutation_indexer.constants import app


class Args(tap.Tap):
    driver: app.Driver

    def _configure(self) -> None:
        self.add_argument("driver", type=app.Driver)


if __name__ == "__main__":
    args = Args().parse_args()

    runpy.run_module(args.driver.module, run_name="__main__")
