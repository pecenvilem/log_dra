import logging
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Iterable
import pandas as pd
from log_dra.config.paths import CONVERTED_FOLDER, MERGED_FOLDER

logger = logging.getLogger(__name__)


def merge_files(input_paths: Iterable[Path], output_path: Path, force: bool = False) -> None:
    """
        Load data from given input_paths, save merged result into file given by output_path. Expecting .parquet files.
        If all input_files are older than the output_path (according to the last modification timestamp),
        don't merge, unless force flag is set.
        Concatenated file may contain DUPLICATED ROWS.
    """
    input_paths = tuple(input_paths)  # ensure that it can be iterated multiple times, if iterator is passed
    try:
        if all_older(input_paths, output_path) and not force:
            logger.info("Input files were not modified since the last modification of the output. Merge NOT performed.")
            return
    except FileNotFoundError:
        logger.debug("Output file not found for comparison, new one will be created.")
        pass
    merged = pd.DataFrame()
    for path in input_paths:
        data = pd.read_parquet(path)
        logger.info(f"Loaded file {path} with {len(data)} rows.")
        merged = pd.concat([data, merged], ignore_index=True)
    merged.to_parquet(output_path)
    logger.info(f"Saved merged data with {len(merged)} rows.")


def all_older(paths: Iterable[Path], than: Path) -> bool:
    """
    Check if all files in the 'paths' have last modification date BEFORE (i. e. are older than) the 'than' file.
    Return bool.
    """
    return max(path.stat().st_mtime for path in paths) <= than.stat().st_mtime


def select_newer(paths: Iterable[Path], than: Path) -> list[Path]:
    """
    Return list (subset of given 'paths') of paths with later modification date than the 'than' path.
    """
    reference = than.stat().st_mtime
    return list(path for path in paths if path.stat().st_mtime > reference)


def merge_folder(source_folder: Path, output_file: Path) -> None:
    """
    Merge all .parquet files in given source_folder and its subfolders into given output_file.
    If output_file exists, only files with later modification date than output_file will be merged.
    """
    all_source_files = (file for file in source_folder.glob("**/*.parquet"))
    t = perf_counter()
    if not output_file.exists():
        logger.info("Output file not found, creating a new one.")
        merge_files(all_source_files, output_file)
    else:
        newer = select_newer(all_source_files, output_file)
        if len(newer) == 0:
            logger.info("Output file up to date.")
            return
        merge_files([output_file, *newer], output_file)
    logger.info(f"Merged folder: {source_folder} into {output_file} file in {perf_counter()-t:.3f}s")


def configure_logger() -> None:
    """
    Configure module-level logger.
    """
    logger.setLevel(logging.DEBUG)
    # noinspection SpellCheckingInspection
    formatter = logging.Formatter('%(levelname)s\t%(asctime)s (%(name)s): %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main():
    """
    Create module-level logger. Merge all files in PARQUET_FOLDER and its subfolders into a file in MERGED_FOLDER.
    Both input and output files are .parquet files.
    The new file is named according to the current date and time.
    """
    configure_logger()
    filename = datetime.now().strftime("%Y%m%d%H%M%S")
    merge_folder(CONVERTED_FOLDER, MERGED_FOLDER / f"{filename}.parquet")


if __name__ == "__main__":
    main()
