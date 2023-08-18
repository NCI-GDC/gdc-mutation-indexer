import runpy
import sys

if __name__ == "__main__":
    driver = sys.argv[1]

    runpy.run_module(driver)
