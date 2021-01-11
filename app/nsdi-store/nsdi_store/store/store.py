import re
import tempfile
import typing
from csv import DictReader
import structlog
from crawler.utils.csv import read_csv
from crawler.aws_client import S3Client
from crawler.utils.converter import convert_land_csv
from crawler.utils.download import extract_zip_file
from loan_model.models.nsdi.nsdi_land_feature import NsdiLandFeature
from loan_model.models.nsdi.nsdi_land_use import NsdiLandUse
from tanker.slack import SlackClient
from tanker.utils.datetime import tzfromtimestamp

from nsdi_store.db import create_session_factory
from .data import (
    NSDI_FEATURE_DICT,
    NSDI_USE_DICT,
)
from .exc import (
    NsdiStoreError,
    NsdiStoreS3NotFound,
    NsdiStoreRegionNotFound,
)

logger = structlog.get_logger(__name__)


class NsdiStore(object):
    def __init__(self, config: typing.Dict[str, typing.Any]) -> None:
        super().__init__()
        self.config = config
        self.session_factory = create_session_factory(config)
        self.s3_client = S3Client(config)
        self.slack_client = SlackClient(
            config.get("SLACK_CHANNEL"), config.get("SLACK_API_TOKEN")
        )
        self.region_level_1 = self.config["REGION_REGEX_LEVEL_1"]
        self.region_level_2 = self.config["REGION_REGEX_LEVEL_2"]

    def run(self, run_by: str) -> None:

        self.slack_client.send_info_slack(
            f"Store 시작합니다. ({self.config['ENVIRONMENT']}, {run_by})"
        )

        crawler_log_id = self.config["CRAWLER_LOG_ID"]

        if crawler_log_id:
            self.fetch_received_log_folder()  # 수동 log id 폴더 저장
        else:
            self.fetch_latest_log_folder()  # 최신 log id 폴더 저장

        self.slack_client.send_info_slack(
            f"Store 종료합니다. ({self.config['ENVIRONMENT']}, {run_by})"
        )

    def fetch_received_log_folder(self) -> None:
        crawler_log_id = self.config["CRAWLER_LOG_ID"]
        crawler_date = tzfromtimestamp(float(crawler_log_id))
        log_id_prefix = (
            f"{self.config['ENVIRONMENT']}/"
            f"{crawler_date.year}/"
            f"{crawler_date.month}/"
            f"{crawler_date.day}/"
            f"{crawler_log_id}/"
        )
        self.fetch_name_type_folder(log_id_prefix)

    def fetch_latest_log_folder(self) -> None:
        env_prefix = f"{self.config['ENVIRONMENT']}/"
        year_prefix = self.fetch_latest_folder(env_prefix)
        month_prefix = self.fetch_latest_folder(year_prefix)
        day_prefix = self.fetch_latest_folder(month_prefix)
        log_id_prefix = self.fetch_latest_folder(day_prefix)
        self.fetch_name_type_folder(log_id_prefix)

    def fetch_latest_folder(self, base_prefix: str) -> str:
        date_list: typing.List[str] = list()
        for response in self.s3_client.get_objects(base_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            if not prefixes:
                raise NsdiStoreS3NotFound("not found date list")
            for date_prefix in prefixes:
                date = (
                    date_prefix["Prefix"]
                    .replace(base_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                date_list.append(date)
            date_list.sort()
        base_prefix += date_list[-1] + "/"

        return base_prefix

    def fetch_name_type_folder(self, log_id_prefix: str) -> None:
        for response in self.s3_client.get_objects(
            log_id_prefix, Delimiter="/"
        ):
            prefixes = response.common_prefixes
            if not prefixes:
                raise NsdiStoreS3NotFound("not found name type folder")
            for name_type_prefix in prefixes:
                name_type = (
                    name_type_prefix["Prefix"]
                    .replace(name_type_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                if "토지이용계획정보" == name_type:
                    self.fetch_sido_region_folder(
                        name_type_prefix['Prefix'],
                        "토지이용계획정보"
                    )
                elif "토지특성정보" == name_type:
                    self.fetch_sido_region_folder(
                        name_type_prefix['Prefix'],
                        "토지특성정보"
                    )

    def fetch_sido_region_folder(
        self, name_type_prefix: str, name_type: str
    ) -> None:
        name_type_prefix += "data/전체데이터/"
        sido_check: bool = False
        for response in self.s3_client.get_objects(
            name_type_prefix, Delimiter="/"
        ):
            prefixes = response.common_prefixes
            if not prefixes:
                raise NsdiStoreS3NotFound("not found sido region list")
            for sido_prefix in prefixes:
                sido_name = (
                    sido_prefix["Prefix"]
                    .replace(name_type_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                if re.search(self.region_level_1, sido_name):
                    sido_check = True
                    self.fetch_gugun_region_folder(
                        sido_prefix["Prefix"], name_type
                    )

        if not sido_check:
            raise NsdiStoreRegionNotFound(
                f"not found sido({self.region_level_1})"
            )

    def fetch_gugun_region_folder(
        self, sido_prefix: str, name_type: str
    ) -> None:
        gugun_check: bool = False
        for response in self.s3_client.get_objects(sido_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            if not prefixes:
                raise NsdiStoreS3NotFound("not found gugun region list")
            for gugun_prefix in prefixes:
                gugun_name = (
                    gugun_prefix["Prefix"]
                    .replace(sido_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                if re.search(self.region_level_2, gugun_name):
                    gugun_check = True
                    self.fetch_base_date_folder(
                        gugun_prefix["Prefix"], name_type
                    )

        if not gugun_check:
            raise NsdiStoreRegionNotFound(
                f"not found gugun({self.region_level_2})"
            )

    def fetch_base_date_folder(
        self, gugun_prefix: str, name_type: str
    ) -> None:
        for response in self.s3_client.get_objects(
            gugun_prefix, Delimiter="/"
        ):
            prefixes = response.common_prefixes
            if not prefixes:
                raise NsdiStoreS3NotFound("not found gugun region list")
            for base_date_prefix in prefixes:
                self.fetch_zip_data_folder(
                    base_date_prefix["Prefix"], name_type
                )

    def fetch_zip_data_folder(
        self, base_date_prefix: str, name_type: str
    ) -> None:
        for response in self.s3_client.get_objects(
            base_date_prefix, Delimiter="/"
        ):
            contents = response.contents
            if not contents:
                raise NsdiStoreS3NotFound("not found statistics data")
            for content in contents:
                file_prefix = content["Key"]
                file_name = (
                    file_prefix.replace(base_date_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                with tempfile.TemporaryDirectory() as temp_dir:
                    folder_path = str(temp_dir) + "/"
                    logger.info(folder_path)
                    file_path = folder_path + file_name
                    logger.info("S3 ZIP DOWNLOAD", file_name=file_name)
                    self.s3_client.download_object(file_prefix, file_path)
                    file_name = file_name.replace(".zip", ".csv")
                    extract_zip_file(file_path, folder_path, file_name)
                    file_path = file_path.replace(".zip", ".csv")

                    converted_csv_path = self.convert_csv_file(
                        file_path, folder_path, file_name, name_type
                    )
                    self.store_csv_data(converted_csv_path, name_type)

    def convert_csv_file(
        self, file_path: str, folder_path: str, file_name: str, name_type: str
    ) -> str:
        logger.info("Convert start", file_name=file_name)
        if name_type == "토지이용계획정보":
            converted_csv_path = convert_land_csv(
                file_path, folder_path, file_name, NSDI_USE_DICT
            )
        elif name_type == "토지특성정보":
            converted_csv_path = convert_land_csv(
                file_path, folder_path, file_name, NSDI_FEATURE_DICT
            )
        else:
            raise NsdiStoreError("not found name type")
        logger.info("Convert finish", file_name=file_name)

        return converted_csv_path

    def store_csv_data(self, file_path: str, name_type: str) -> None:
        """
        우선은 10,000줄 단위로 읽은 후 upsert하도록 하였습니다
        만약 bulk insert를 원할경우 store_bulk_insert 메소드를 사용해주세요
        """
        rows = list()
        if name_type == "토지이용계획정보":
            # self.store_land_use_bulk_insert(file_path)
            for idx, row in enumerate(read_csv(file_path)):
                rows.append(row)
                if idx % 10000 == 0 and idx != 0:
                    self.store_land_use_bulk_upsert(rows)
                    rows.clear()
            if rows:
                self.store_land_use_bulk_upsert(rows)
        elif name_type == "토지특성정보":
            # self.store_land_feature_bulk_insert(file_path)
            for idx, row in enumerate(read_csv(file_path)):
                rows.append(row)
                if idx % 10000 == 0 and idx != 0:
                    self.store_land_feature_bulk_upsert(rows)
                    rows.clear()
            if rows:
                self.store_land_feature_bulk_upsert(rows)

    def store_land_use_bulk_insert(self, file_path: str) -> None:
        """
        csv 파일을 한번에 읽어서 bulk insert를 해줍니다
        """
        bulk_values: typing.List[typing.Dict[str, str]]
        session = self.session_factory()
        with open(file_path, "r", encoding="utf-8-sig") as csv_file:
            csv_dict_reader = DictReader(csv_file)
            bulk_values = list(csv_dict_reader)
        logger.info("bulk insert start")
        try:
            session.bulk_insert_mappings(NsdiLandUse, bulk_values)
            session.commit()
        except Exception:
            raise NsdiStoreError("Store land use error")
        finally:
            session.close()
        logger.info("bulk insert finish")

    def store_land_feature_bulk_insert(self, file_path: str) -> None:
        """
        csv 파일을 한번에 읽어서 bulk insert를 해줍니다
        """
        bulk_values: typing.List[typing.Dict[str, str]]
        session = self.session_factory()
        with open(file_path, "r", encoding="utf-8-sig") as csv_file:
            csv_dict_reader = DictReader(csv_file)
            bulk_values = list(csv_dict_reader)
        logger.info("bulk insert start")
        try:
            session.bulk_insert_mappings(NsdiLandFeature, bulk_values)
            session.commit()
        except Exception:
            raise NsdiStoreError("Store land feature error")
        finally:
            session.close()
        logger.info("bulk insert finish")

    def store_land_use_bulk_upsert(self, bulk_values: list) -> None:
        """
        이 부분을 사용하려면 loan-model에 bulk_create_or_update를 추가해야합니다.
        """
        session = self.session_factory()
        logger.info("bulk upsert start")
        try:
            NsdiLandUse.bulk_create_or_update(session, bulk_values)
            session.commit()
        except Exception:
            raise NsdiStoreError("Store land use error")
        finally:
            session.close()
        logger.info("bulk upsert finish")

    def store_land_feature_bulk_upsert(self, bulk_values: list) -> None:
        """
        이 부분을 사용하려면 loan-model에 bulk_create_or_update를 추가해야합니다.
        """
        session = self.session_factory()
        logger.info("bulk upsert start")
        try:
            NsdiLandFeature.bulk_create_or_update(session, bulk_values)
            session.commit()
        except Exception:
            raise NsdiStoreError("Store land feature error")
        finally:
            session.close()
        logger.info("bulk upsert finish")
