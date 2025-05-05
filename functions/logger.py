import datetime
import logging
from pathlib import Path
from config import LOG_DIR


def setup_logger(log_dir: str = LOG_DIR) -> logging.Logger:
    """
    Sets up a logger with a timestamped log file and separate console/file log levels.

    Parameters:
        log_dir (str): Directory to store log files.

    Returns:
        logging.Logger: Configured logger instance.
    """
    Path(log_dir).mkdir(exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"run_{timestamp}.log"

    logger = logging.getLogger("CBSLogger")
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # File handler
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger

def cleanup_old_logs(log_dir: str = "logs", days: int = 7) -> None:
    """
    Deletes log files older than a specified number of days.

    Parameters:
        log_dir (str): Path to the log directory.
        days (int): Number of days to keep logs.
        :rtype: object
    """
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
    log_path = Path(log_dir)

    if not log_path.exists():
        return

    for file in log_path.glob("*.log"):
        try:
            file_mtime = datetime.datetime.fromtimestamp(file.stat().st_mtime)
            if file_mtime < cutoff_date:
                file.unlink()
                print(f"Deleted old log file: {file.name}")
        except Exception as e:
            print(f"Failed to delete {file.name}: {e}")