from datetime import datetime
from time import perf_counter

from logging import getLogger
from logging.config import dictConfig
from log_dra.config.pipeline_logging_config import config

from log_dra.config.paths import XLSX_FOLDER, CONVERTED_FOLDER, MERGED_FOLDER, PROCESSED_FOLDER
from log_dra.pipeline.convert import convert_folder
from log_dra.pipeline.merge import merge_folder, all_older
from log_dra.pipeline.analyse import analyse_file

logger = getLogger(__name__)
dictConfig(config)


def main() -> None:
    # noinspection SpellCheckingInspection
    """
    Run the whole data processing pipeline described in doc/pipeline.puml and doc/pilepine_logic.puml
    """
    logger.info("Starting conversion")
    t = perf_counter()
    convert_folder(XLSX_FOLDER, CONVERTED_FOLDER)
    logger.info(f"Conversion done in {perf_counter()-t:.3f}s")

    logger.info("Starting merge")
    t = perf_counter()
    recent_merge_file, *_ = sorted(MERGED_FOLDER.glob("*.parquet"), key=lambda file: file.stat().st_mtime, reverse=True)
    logger.info(f"Last merged file: {recent_merge_file}")
    if not all_older(CONVERTED_FOLDER.glob("**/*.parquet"), recent_merge_file):
        filename = datetime.now().strftime("%Y%m%d%H%M%S")
        logger.debug(f"Foud new data, creating new merged file: {filename}")
        merge_folder(CONVERTED_FOLDER, MERGED_FOLDER / f"{filename}.parquet")
    else:
        logger.info("No new data found, merge not run.")
    logger.info(f"Merge done in: {perf_counter()-t:.3f}s")

    logger.info("Starting analysis")
    t = perf_counter()
    for merge_file in MERGED_FOLDER.glob("*.parquet"):
        logger.info(f"Analysing file: {merge_file}")
        analyse_file(merge_file, PROCESSED_FOLDER / merge_file.name, smart=True)
    logger.info(f"Analysis done in: {perf_counter()-t:.3f}")


if __name__ == '__main__':
    main()
