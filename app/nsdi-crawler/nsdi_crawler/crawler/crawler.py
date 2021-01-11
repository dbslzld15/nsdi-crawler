import datetime
import json
import tempfile
import typing

import attr
import pytz
import structlog
from crawler import resource
from crawler.aws_client import S3Client
from crawler.utils.download import download_from_response
from tanker.slack import SlackClient
from tanker.utils.datetime import tznow, timestamp

from nsdi_crawler.client import NsdiClient
from nsdi_crawler.client.data import NsdiLandUsingInfo
from .data import (
    CrawlerStatistics,
    CrawlerRegionDate,
    CrawlerLogResponse,
    slack_failure_percentage_statistics,
)
from .exc import NsdiCrawlerNotFoundError

logger = structlog.get_logger(__name__)

SeoulTZ = pytz.timezone("Asia/Seoul")


class NsdiCrawler(object):
    def __init__(
        self,
        config: typing.Dict[str, typing.Any],
    ) -> None:
        super().__init__()
        self.config = config
        self.slack_client = SlackClient(
            config.get("SLACK_CHANNEL"), config.get("SLACK_API_TOKEN")
        )
        self.nsdi_client = NsdiClient(config)
        self.s3_client = S3Client(config)
        self.region_land_use_dict: typing.Dict[str, str] = dict()
        self.region_land_feature_dict: typing.Dict[str, str] = dict()
        self.total_statistics = CrawlerStatistics()
        self.failure_statistics = CrawlerStatistics()
        self.crawling_date: datetime.datetime = tznow(
            pytz.timezone("Asia/Seoul")
        )
        self.crawling_start_time: str = str(
            timestamp(tznow(pytz.timezone("Asia/Seoul")))
        )

    def run(self, run_by: str) -> None:
        """
        토지이용계획정보와 토지특성정보 2가지를 크롤링합니다.
        만약 크롤러 로그가 S3에 없다면 2019-01-01 이후 데이터를 크롤링 합니다.
        만약 크롤러 로그가 있다면 로그를 통해 지역별로 날짜를 비교하며 중복데이터는 저장하지 않습니다
            현재 날짜 기준 6개월 전으로 시작날짜를 잡고 크롤링 합니다.
        크롤러 로그는 정상적으로 모든 프로세스가 완료되었을때만 작성됩니다.
        수집한 데이터가 없어도 크롤러 로그는 항상 s3에 지역별 최신으로 올려줍니다.
        """

        self.slack_client.send_info_slack(
            f"TIME_STAMP: {self.crawling_start_time}\n"
            f"업데이트할 데이터를 찾습니다"
            f"({self.config['ENVIRONMENT']}, {run_by})"
        )

        self.crawl(run_by)

        statistics = slack_failure_percentage_statistics(
            self.total_statistics, self.failure_statistics
        )

        if (
            self.total_statistics.land_use_zip_count == 0
            and self.total_statistics.land_feature_zip_count == 0
        ):
            self.slack_client.send_info_slack(
                f"업데이트할 데이터가 없습니다\n"
                f"TIME_STAMP: {self.crawling_start_time}\n\n"
                f"statistics: {statistics}",
            )
        else:
            self.slack_client.send_info_slack(
                f"데이터 업데이트 완료\n"
                f"TIME_STAMP: {self.crawling_start_time}\n\n"
                f"statistics: {statistics}",
            )

    def crawl(self, run_by: str) -> None:
        land_use_log_none = self.fetch_region_crawler_log(
            prov_org="NIDO",
            gubun="F",
            svc_se="F",
            svc_id="F014",
            name_type="토지이용계획정보",
        )

        land_feature_log_none = self.fetch_region_crawler_log(
            prov_org="SCOS",
            gubun="F",
            svc_se="F",
            svc_id="F024",
            name_type="토지특성정보",
        )

        try:
            self.crawl_land_use(
                prov_org="NIDO",
                svc_se="F",
                svc_id="F014",
                crawler_log_none=land_use_log_none,
            )
        except NsdiCrawlerNotFoundError:
            logger.info("해당하는 날짜의 데이터가 없습니다")

        try:
            self.crawl_land_feature(
                prov_org="SCOS",
                svc_se="F",
                svc_id="F024",
                crawler_log_none=land_feature_log_none,
            )
        except NsdiCrawlerNotFoundError:
            logger.info("해당하는 날짜의 데이터가 없습니다")

        if self.total_statistics.land_use_zip_count > 0:
            self.update_crawler_log(run_by, "토지이용계획정보")

        if self.total_statistics.land_feature_zip_count > 0:
            self.update_crawler_log(run_by, "토지특성정보")

        if (
            self.total_statistics.land_use_zip_count == 0
            and self.total_statistics.land_feature_zip_count != 0
        ):
            self.update_crawler_log(run_by, "토지이용계획정보없음")
        if (
            self.total_statistics.land_feature_zip_count == 0
            and self.total_statistics.land_use_zip_count != 0
        ):
            self.update_crawler_log(run_by, "토지특성정보없음")

    def crawl_land_use(
        self,
        *,
        prov_org: str,
        svc_se: str,
        svc_id: str,
        crawler_log_none: bool,
    ) -> None:  # 토지이용계획정보 크롤링
        data_type = self.config["DATA_TYPE"]
        extrc_se_search = "AL" if data_type == "전체데이터" else "CH"

        if crawler_log_none:
            start_date = "2019-01-01"
        else:
            start_date = (
                self.crawling_date - datetime.timedelta(weeks=25)
            ).strftime("%Y-%m-%d")

        end_date = (self.crawling_date + datetime.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )

        self.crawl_land_info(
            svc_se, svc_id, start_date, end_date, extrc_se_search, prov_org
        )

        if self.total_statistics.land_use_zip_count == 0:
            logger.info("수집된 데이터가 없습니다")

    def crawl_land_feature(
        self,
        *,
        prov_org: str,
        svc_se: str,
        svc_id: str,
        crawler_log_none: bool,
    ) -> None:  # 토지특성정보
        data_type = self.config["DATA_TYPE"]
        extrc_se_search = "AL" if data_type == "전체데이터" else "CH"

        if crawler_log_none:
            start_date = "2019-01-01"
        else:
            start_date = (
                self.crawling_date - datetime.timedelta(weeks=25)
            ).strftime("%Y-%m-%d")

        end_date = (self.crawling_date + datetime.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )

        self.crawl_land_info(
            svc_se, svc_id, start_date, end_date, extrc_se_search, prov_org
        )

        if self.total_statistics.land_feature_zip_count == 0:
            logger.info("수집된 데이터가 없습니다")

    def crawl_land_info(
        self,
        svc_se: str,
        svc_id: str,
        start_date: str,
        end_date: str,
        extrc_se_search: str,
        prov_org: str,
    ) -> None:

        try:
            page = self.nsdi_client.fetch_land_using_info_table(
                svc_se,
                svc_id,
                start_date,
                end_date,
                extrc_se_search,
                prov_org,
                1,
            )
        except TypeError:
            raise NsdiCrawlerNotFoundError("해당하는 날짜의 데이터가 없습니다")

        with tempfile.TemporaryDirectory() as temp_dir:  # 임시 디렉토리 설정
            for i in range(page.total_page, 0, -1):
                info_list = self.nsdi_client.fetch_land_using_info_table(
                    svc_se,
                    svc_id,
                    start_date,
                    end_date,
                    extrc_se_search,
                    prov_org,
                    i,
                ).land_using_info

                for info in reversed(info_list):
                    region_dict_date = info.base_date
                    if info.city_type == "인천광역시 남구":
                        if info.name_type == "토지이용계획정보":
                            region_dict_date = self.region_land_use_dict[
                                "인천광역시 미추홀구"
                            ]
                        elif info.name_type == "토지특성정보":
                            region_dict_date = self.region_land_feature_dict[
                                "인천광역시 미추홀구"
                            ]
                    else:
                        if info.name_type == "토지이용계획정보":
                            region_dict_date = self.region_land_use_dict[
                                info.city_type
                            ]
                        elif info.name_type == "토지특성정보":
                            region_dict_date = self.region_land_feature_dict[
                                info.city_type
                            ]
                    log_datetime = datetime.datetime.strptime(
                        region_dict_date, "%Y-%m-%d"
                    )
                    nsdi_datetime = datetime.datetime.strptime(
                        info.base_date, "%Y-%m-%d"
                    )

                    if nsdi_datetime > log_datetime and (
                        info.name_type == "토지이용계획정보"
                        or info.name_type == "토지특성정보"
                    ):  # 로그에 비해 최신이면 다운로드
                        logger.info(
                            "Crawling Data",
                            data_type=info.data_type,
                            city_type=info.city_type,
                            name_type=info.name_type,
                            base_date=info.base_date,
                            file_size=info.file_size,
                        )
                        self.download_zip_data(info, temp_dir, prov_org)
                        if info.name_type == "토지이용계획정보":
                            self.region_land_use_dict[
                                info.city_type
                            ] = info.base_date
                        elif info.name_type == "토지특성정보":
                            self.region_land_feature_dict[
                                info.city_type
                            ] = info.base_date

                    else:
                        continue

    def download_zip_data(
        self,
        nsdi_land_using_info: NsdiLandUsingInfo,
        temp_dir: str,
        prov_org: str,
    ) -> None:
        table_data = nsdi_land_using_info.table_data
        temp_path = str(temp_dir) + "\\"
        file_name = table_data.file_nm_dialog

        if self.config["DOWNLOAD"] == "ON":
            # 압축 파일 다운로드
            response = self.nsdi_client.fetch_download_response(
                table_data, prov_org
            )
            download_from_response(temp_path, file_name, response)
            path = temp_path + file_name
        else:
            path = resource.get_resource("/csv/nsdi_csv.zip")

        self.upload_zip_data(nsdi_land_using_info, path)

        if nsdi_land_using_info.name_type == "토지이용계획정보":
            self.total_statistics.land_use_zip_count += 1
        elif nsdi_land_using_info.name_type == "토지특성정보":
            self.total_statistics.land_feature_zip_count += 1

    def upload_zip_data(
        self, nsdi_land_using_info: NsdiLandUsingInfo, temp_path: str
    ) -> None:

        if len(nsdi_land_using_info.city_type.split()) > 1:  # 시,군,구 데이터일때
            region_split = nsdi_land_using_info.city_type.split()
            sido_name = region_split[0]
            gugun_name = nsdi_land_using_info.city_type.replace(sido_name, "")
        else:  # 시,도 데이터일때
            sido_name = nsdi_land_using_info.city_type
            gugun_name = "ALL"

        folder_name = (
            f"{self.config['ENVIRONMENT']}/"
            f"{self.crawling_date.year}/"
            f"{self.crawling_date.month:02}/"
            f"{self.crawling_date.day:02}/"
            f"{str(self.crawling_start_time)}/"
            f"{nsdi_land_using_info.name_type}/"
            f"data/"
            f"{nsdi_land_using_info.data_type}/"
            f"{sido_name}/"
            f"{gugun_name}/"
            f"base_date_{nsdi_land_using_info.base_date}"
        )
        file_name = nsdi_land_using_info.table_data.file_nm_dialog

        self.s3_client.upload_s3_zip(
            folder_name=folder_name,
            file_name=file_name,
            temp_path=temp_path,
            mime_type="application/zip",
        )

    def update_crawler_log(self, run_by: str, name_type: str) -> None:
        """
        크롤러 로그는 기존 크롤러 로그를 업데이트하는 방식으로 작성되어집니다.
        """
        region_date_list: typing.List[CrawlerRegionDate] = []
        total_statistics = attr.asdict(self.total_statistics)
        if name_type == "토지이용계획정보":
            total_statistics["land_feature_zip_count"] = 0
        elif name_type == "토지특성정보":
            total_statistics["land_use_zip_count"] = 0
        elif name_type == "토지이용계획정보없음":
            total_statistics["land_feature_zip_count"] = 0
            total_statistics["land_use_zip_count"] = 0
            name_type = "토지이용계획정보"
        elif name_type == "토지특성정보없음":
            total_statistics["land_feature_zip_count"] = 0
            total_statistics["land_use_zip_count"] = 0
            name_type = "토지특성정보"

        if name_type == "토지이용계획정보":
            for region, date in self.region_land_use_dict.items():
                if date != "0001-01-01":
                    region_date_list.append(
                        CrawlerRegionDate(region=region, date=date)
                    )
        elif name_type == "토지특성정보":
            for region, date in self.region_land_feature_dict.items():
                if date != "0001-01-01":
                    region_date_list.append(
                        CrawlerRegionDate(region=region, date=date)
                    )

        data = {
            "time_stamp": self.crawling_start_time,
            "run_by": run_by,
            "finish_time_stamp": str(timestamp(tznow())),
            "total_statistics": total_statistics,
            "region_date": [vars(x) for x in region_date_list],
        }

        folder_name = (
            f"{self.config['ENVIRONMENT']}/"
            f"{self.crawling_date.year}/"
            f"{self.crawling_date.month:02}/"
            f"{self.crawling_date.day:02}/"
            f"{str(self.crawling_start_time)}/"
            f"{name_type}/"
            f"crawler-log"
        )

        file_name = f"{self.crawling_start_time}.json"

        self.s3_client.upload_s3(
            folder_name, file_name, data, "application/json", encoding="utf-8"
        )

    def fetch_region_crawler_log(
        self,
        *,
        prov_org: str,
        gubun: str,
        svc_se: str,
        svc_id: str,
        name_type: str,
    ) -> bool:
        """
        지역별 날짜를 딕셔너리에 저장하고 크롤러 로그의 유무를 반환합니다.
        토지특성정보 데이터의 경우에는 시,군,구에 대한 최신 데이터 날짜도 가져옵니다.
        """
        response = self.nsdi_client.init_page(prov_org, gubun, svc_se, svc_id)
        region_list = self.nsdi_client.fetch_region_list(response)

        for region in region_list:
            if name_type == "토지특성정보":
                self.region_land_feature_dict.update(
                    {region.adm_code_nm: "0001-01-01"}
                )
                region_detail_list = self.nsdi_client.fetch_region_detail_list(
                    region.adm_code
                )
                for region_detail in region_detail_list:
                    self.region_land_feature_dict.update(
                        {region_detail.adm_code_nm: "0001-01-01"}
                    )
            elif name_type == "토지이용계획정보":
                self.region_land_use_dict.update(
                    {region.adm_code_nm: "0001-01-01"}
                )

        try:
            crawler_log = self.fetch_crawler_log(name_type)
            crawler_log_none = False
            for sido in crawler_log.region_date:
                if name_type == "토지이용계획정보":
                    self.region_land_use_dict[sido.region] = sido.date
                elif name_type == "토지특성정보":
                    self.region_land_feature_dict[sido.region] = sido.date
        except TypeError:
            crawler_log_none = True

        return crawler_log_none

    def fetch_crawler_log(self, name_type: str) -> CrawlerLogResponse:
        log_id_prefix = self.fetch_crawler_log_path(name_type)
        response = self.s3_client.get_object(log_id_prefix)
        json_log = json.loads(response.body.read())
        return CrawlerLogResponse.from_json(json_log)

    def fetch_crawler_log_path(self, name_type: str) -> str:  # 최신 로그 폴더 경로
        env_prefix = f"{self.config['ENVIRONMENT']}/"
        year_list: typing.List[str] = []
        month_list: typing.List[str] = []
        day_list: typing.List[str] = []
        time_stamp_list: typing.List[str] = []
        log_id_list: typing.List[str] = []

        for response in self.s3_client.get_objects(env_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            for year_prefix in prefixes:
                year = (
                    year_prefix["Prefix"]
                    .replace(env_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                year_list.append(year)
            year_list.sort()
        year_prefix = env_prefix + year_list[-1] + "/"

        for response in self.s3_client.get_objects(year_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            for month_prefix in prefixes:
                month = (
                    month_prefix["Prefix"]
                    .replace(year_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                month_list.append(month)
            month_list.sort()
        month_prefix = year_prefix + month_list[-1] + "/"

        for response in self.s3_client.get_objects(
            month_prefix, Delimiter="/"
        ):
            prefixes = response.common_prefixes

            for day_prefix in prefixes:
                day = (
                    day_prefix["Prefix"]
                    .replace(month_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                day_list.append(day)
            day_list.sort()
        day_prefix = month_prefix + day_list[-1] + "/"

        for response in self.s3_client.get_objects(day_prefix, Delimiter="/"):
            prefixes = response.common_prefixes
            for time_stamp_prefix in prefixes:
                time_stamp = (
                    time_stamp_prefix["Prefix"]
                    .replace(day_prefix, "")
                    .replace("/", "")
                    .strip()
                )
                time_stamp_list.append(time_stamp)
            time_stamp_list.sort()

        time_stamp_prefix = day_prefix + time_stamp_list[-1] + "/"

        log_id_prefix = f"{time_stamp_prefix}" f"{name_type}/" f"crawler-log/"

        for response in self.s3_client.get_objects(log_id_prefix):
            for content in response.contents:
                log_id = content["Key"].split("/")[-1].replace(".json", "")
                log_id_list.append(log_id)
            log_id_list.sort()

        log_id_prefix += log_id_list[-1] + ".json"

        return log_id_prefix
