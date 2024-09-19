import logging

from mutation_indexer.gene_expression import driver

logger = logging.getLogger()

if __name__ == "__main__":
    try:
        driver.Driver.run()
    except Exception as ex:
        logger.critical("Gene expression build failed", exc_info=ex)
