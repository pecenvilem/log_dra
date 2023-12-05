from log_dra.config.paths import LOGS_FOLDER

config = {
    "version": 1,
    "formatters": {
        "standard": {""
                     "format": "%(levelname)s\t%(asctime)s (%(name)s): %(message)s"}
    },
    "handlers": {
        "file": {"class": "logging.FileHandler", "formatter": "standard", "filename": LOGS_FOLDER / "pipeline.log"},
        "console": {"class": "logging.StreamHandler", "formatter": "standard"},
    },
    "loggers": {
        "__main__": {"level": "DEBUG", "handlers": ["file", "console"]},
        "log_dra.pipeline.convert": {"level": "DEBUG", "handlers": ["file", "console"]},
        "log_dra.pipeline.merge": {"level": "DEBUG", "handlers": ["file", "console"]},
        "log_dra.pipeline.analyse": {"level": "DEBUG", "handlers": ["file", "console"]},
    },
}
