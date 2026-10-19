# /// script
# requires-python = "~=3.13.0"
# dependencies = []
# ///
"""A wrapper script for calling the desired driver module."""

import runpy

if __name__ == "__main__":
    runpy.run_module("mutation_indexer.$MODULE", run_name="__main__")
