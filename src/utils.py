import dotenv
import logging
from pathlib import Path
import pandas as pd
import sys

def load_env():
    dotenv.load_dotenv()

def setup_logging(log_file: Path):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="a"),
        ],
    )

def read_tsv(
    path: Path,
    dtype=None,
    index_cols=None
    ):
    """
    Read a TSV into a DataFrame.
    If index_cols is provided, use them as the index (can be list or str).
    """
    if not path.exists():
        return None
    if dtype:
        df = pd.read_csv(path, sep='\t', dtype=dtype)
    else:
        df = pd.read_csv(path, sep='\t', dtype=str)  # read everything as str to ease comparison
    if index_cols:
        df = df.set_index(index_cols)
    return df
