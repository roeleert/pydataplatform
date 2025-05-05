import logging
import os
from pathlib import Path

logger = logging.getLogger("CBSLogger")


def create_directory(directory_path: Path) -> None:
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"Created directory: {directory_path}")
        else:
            logger.debug(f"Directory already exists: {directory_path}")
    except OSError as e:
        logger.error(f"Error creating directory {directory_path}: {e}", exc_info=True)


def empty_directory(directory_path: Path) -> None:
    try:
        if os.path.exists(directory_path):
            for filename in os.listdir(directory_path):
                file_path = os.path.join(directory_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                        logger.debug(f"Deleted file: {file_path}")
                    elif os.path.isdir(file_path):
                        os.rmdir(file_path)
                        logger.debug(f"Deleted subdirectory: {file_path}")
                except Exception as e:
                    logger.warning(f"Error removing {file_path}: {e}", exc_info=True)
            logger.info(f"Emptied directory: {directory_path}")
        else:
            logger.warning(f"Directory does not exist: {directory_path}")
    except Exception as e:
        logger.error(f"Error emptying directory {directory_path}: {e}", exc_info=True)
