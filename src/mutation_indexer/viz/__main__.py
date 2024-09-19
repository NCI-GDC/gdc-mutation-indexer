import logging

from mutation_indexer.viz import driver

logger = logging.getLogger()

if __name__ == "__main__":
    try:
        driver.Driver.run()
    except Exception as ex:
        logger.critical("Viz build failed", exc_info=ex)
