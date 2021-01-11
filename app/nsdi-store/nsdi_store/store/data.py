import typing
import attr

NSDI_FEATURE_DICT = {
    "고유번호": "pnu",
    "법정동코드": "",
    "법정동명": "address_jibun",
    "대장구분코드": "ledger_kind_code",
    "대장구분명": "ledger_kind_name",
    "지번": "bunji",
    "토지일련번호": "",
    "기준년도": "",
    "기준월": "",
    "지목코드": "land_category_code",
    "지목명": "land_category_name",
    "토지면적": "land_area",
    "용도지역코드1": "land_use_code",
    "용도지역명1": "land_use_name",
    "용도지역코드2": "land_use_code2",
    "용도지역명2": "land_use_name2",
    "토지이용상황코드": "land_using_code",
    "토지이동상황": "land_using_name",
    "지형높이코드": "terrain_height_code",
    "지형높이": "terrain_height_name",
    "지형형상코드": "terrain_shape_code",
    "지형형상": "terrain_shape_name",
    "도로접면코드": "doro_neighbor_code",
    "도로접면": "doro_neighbor_name",
    "공시지가": "land_declared_value",
    "데이터기준일자": "last_update_date",
}

NSDI_USE_DICT = {
    "고유번호": "pnu",
    "관리번호": "",
    "법정동코드": "",
    "법정동명": "address_jibun",
    "대장구분코드": "ledger_kind_code",
    "대장구분명": "ledger_kind_name",
    "지번": "bunji",
    "도면번호": "",
    "저촉여부코드": "border_neighbor_code",
    "저촉여부": "border_neighbor_name",
    "용도지역지구코드": "land_use_code",
    "용도지역지구명": "land_use_name",
    "등록일자": "",
    "데이터기준일자": "last_update_date",
}


@attr.s
class CrawlerRegionDate(object):
    region: str = attr.ib()
    date: str = attr.ib()

    class CrawlerRegionDateData(typing.Dict):
        region: str
        date: str

    @classmethod
    def from_json(cls, data: CrawlerRegionDateData) -> "CrawlerRegionDate":
        return cls(region=data["region"], date=data["date"])


@attr.s
class CrawlerStatistics(object):
    #: 수집한 ZIP 갯수
    land_use_zip_count: int = attr.ib(default=0)
    land_feature_zip_count: int = attr.ib(default=0)

    class CrawlerStatisticsData(typing.Dict):
        land_use_zip_count: int
        land_feature_zip_count: int

    @classmethod
    def from_json(cls, data: CrawlerStatisticsData) -> "CrawlerStatistics":
        return cls(
            land_use_zip_count=data["land_use_zip_count"],
            land_feature_zip_count=data["land_feature_zip_count"],
        )


@attr.s(frozen=True)
class CrawlerLogResponse(object):
    time_stamp: float = attr.ib()
    run_by: str = attr.ib()
    finish_time_stamp: float = attr.ib()
    total_statistics: CrawlerStatistics = attr.ib()
    region_date: typing.List[CrawlerRegionDate] = attr.ib()

    class CrawlerLogResponseData(typing.Dict):
        time_stamp: str
        run_by: str
        finish_time_stamp: str
        total_statistics: CrawlerStatistics.CrawlerStatisticsData
        region_date: typing.List[CrawlerRegionDate.CrawlerRegionDateData]

    @classmethod
    def from_json(cls, data: CrawlerLogResponseData) -> "CrawlerLogResponse":
        return cls(
            time_stamp=float(data["time_stamp"]),
            run_by=data["run_by"],
            finish_time_stamp=float(data["finish_time_stamp"]),
            total_statistics=CrawlerStatistics.from_json(
                data["total_statistics"]
            ),
            region_date=[
                CrawlerRegionDate.from_json(x) for x in data["region_date"]
            ],
        )
