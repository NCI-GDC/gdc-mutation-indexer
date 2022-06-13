import argparse
import runpy

from mutation_indexer.core.constants import master


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--driver", type=master.Driver)

    return parser


def main(driver: master.Driver) -> None:
    driver_module = f"mutation_indexer.{driver.name.lower()}"

    runpy.run_module(driver_module, init_globals={}, run_name="__main__")


if __name__ == "__main__":
    parser = get_argument_parser()
    args = parser.parse_args()

    main(args.driver)
