import logging
import os
from time import perf_counter

from log_dra.config.paths import XLSX_FOLDER, CONVERTED_FOLDER
import pandas as pd
from pathlib import Path
import warnings

logger = logging.getLogger(__name__)


def convert_file(input_file: Path, output_file: Path,
                 header_rows: int = 1, force_overwrite: bool = False, force_verify_content: bool = False) -> None:
    """
    Check if the last modification of output_file was after the last modification of input_file.
    If yes, consider the output_file up to date and don't convert.
    If input file was modified later than the output, compare their contents. Don't convert, unless they differ.
    Comparison of contents can be forced when passing force_verify_content as True.
    If force_overwrite is passed as True, no checks are made and file is converted immediately.
    """

    def load() -> pd.DataFrame:
        """
        Load and return contents of the input file as DataFrame. Supress warnings.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return pd.read_excel(input_file, header=header_rows)

    def verify_and_convert() -> None:
        """
        Check if output_file already exists. If yes, check if it's content is identical to the content of input file.
        If yes don't convert.
        If contents are different or the output file is not found, convert.
        """
        data = load()
        try:
            reference = pd.read_parquet(output_file)
        except FileNotFoundError:
            logger.debug("File for content verification not found.")
            data.to_parquet(output_file)
            logger.info(f"Output file created.")
        else:
            if data.equals(reference):
                os.utime(output_file)
                logger.info("File contents are identical. Updated output file modification date to now.")
            else:
                data.to_parquet(output_file)
                logger.info(f"Output file overwritten.")

    if force_overwrite:
        logger.debug("Forced overwrite with no checks.")
        if output_file.exists():
            load().to_parquet(output_file)
            logger.info(f"Output file overwritten.")
        else:
            load().to_parquet(output_file)
            logger.info(f"Output file created.")
        return

    if force_verify_content:
        logger.debug("Forced content verification.")
        verify_and_convert()

    try:
        if input_file.stat().st_mtime <= output_file.stat().st_mtime:
            logger.info(f"Input file was not modified since the last change to output.")
            return
    except FileNotFoundError:
        # output_file doesn't exist yet, it will be created
        pass

    verify_and_convert()


def convert_folder(input_folder: Path, output_folder: Path) -> None:
    """
    Go through all .xlsx files in input_folder and it's subdirectories. Convert them into .parquet files with matching
    paths relative to the output_folder. Skip conversion, if files are up to date.
    """
    logger.debug(f"Conversion source folder: {input_folder}")
    logger.debug(f"Conversion output folder: {output_folder}")
    for path in input_folder.glob("**/*.xlsx"):
        logger.info(f"Converting file: {path.relative_to(input_folder)}.")
        *_, year, month, filename = path.parts
        new_filename = filename.replace(".xlsx", ".parquet")
        new_dir = output_folder / year / month
        new_dir.mkdir(parents=True, exist_ok=True)
        new_path = new_dir / new_filename
        t = perf_counter()
        convert_file(path, new_path)
        logger.info(f"Finished converting in {perf_counter()-t:.3f}s")


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
    Configure module-level logger. Convert all .xlsx files from XLSX_FOLDER into corresponding files in PARQUET_FOLDER.
    """
    configure_logger()
    convert_folder(XLSX_FOLDER, CONVERTED_FOLDER)


if __name__ == "__main__":
    main()
