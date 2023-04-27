import logging, os, time
from collections import defaultdict

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
import pandas as pd

from kf_utils.dataservice.descendants import *


logging.basicConfig(level=logging.INFO)

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)


class Ingest:
    def __init__(self, kf_study_ids):
        """A constructor method.

        :param kf_study_ids: a list of KF study IDs
        :type kf_study_ids: list
        """
        self.kf_study_ids = kf_study_ids
        self.kf_dataservice_db_url = os.getenv("KF_DATASERVICE_DB_URL")
        self.all_targets = defaultdict()

    def _create_snapshot(self, kf_study_ids):
        """Creates a study's snapshot from the KF dataservice DB.

        :param kf_study_ids: a list of KF study IDs
        :type kf_study_ids: list
        :return: a snapshot of KF studies
        :rtype: dict
        """
        con = create_engine(self.kf_dataservice_db_url)
        snapshot = defaultdict()
        expected, found = len(kf_study_ids), 0

        # Loop over KF study IDs
        for kf_study_id in self.kf_study_ids:
            logging.info(f"  ⏳ Attempting to acquire {kf_study_id} for snapshot.")
            # study
            study = pd.read_sql(
                f"SELECT * FROM study WHERE kf_id = '{kf_study_id}'", con
            )
            if not study.shape[0] > 0:
                raise Exception(f"{kf_study_id} not found")

            # investigator
            investigator_id = study.investigator_id.tolist()[0]
            investigator = None
            if investigator_id:
                investigator = pd.read_sql(
                    f"SELECT * FROM investigator WHERE kf_id = '{investigator_id}'", con
                )

            # descendants
            logging.info(f"  ⏳ Going for {kf_study_id} for descendants.")
            descendants = find_descendants_by_kfids(
                self.kf_dataservice_db_url,
                "studies",
                kf_study_id,
                ignore_gfs_with_hidden_external_contribs=False,
                kfids_only=False,
            )
            descendants["studies"] = study
            if investigator is not None:
                descendants["investigators"] = investigator

            # Cache a study in memory
            snapshot[kf_study_id] = descendants
            found += 1

        assert expected == found, f"Found {found} study(ies) but expected {expected}"

        return snapshot

    def extract(self):
        """Extracts records.

        :return: A dictionary mapping an endpoint to records
        :rtype: dict
        """
        snapshot = self._create_snapshot(self.kf_study_ids)
        mapped_df_dict = defaultdict()

        for kf_study_id, descendants in snapshot.items():
            logging.info(f"  ⏳ Extracting {kf_study_id}")

            # Loop over descendants
            for endpoint, records in descendants.items():
                df = None
                if endpoint in {"investigators", "studies"}:
                    df = records
                else:
                    df = pd.DataFrame.from_dict(records, orient="index")
                df = df.drop(columns=["uuid","modified_at"])
                mapped_df_dict.setdefault(kf_study_id, {})[endpoint] = df
                logging.info(f"    📁 {endpoint} {df.shape}")

            logging.info(f"  ✅ Extracted {kf_study_id}")

        return mapped_df_dict