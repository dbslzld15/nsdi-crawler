import typing

import attr


@attr.s
class CrawlerRegionDate(object):
    region: str = attr.ib()
    date: str = attr.ib()

    class CrawlerRegionDateData(typing.Dict):
        region: str
        date: str

    @classmethod
    def from_json(cls, data: CrawlerRegionDateData
                  ) -> "CrawlerRegionDate":
        return cls(
            region=data["region"],
            date=data["date"]
        )


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
        region_date: typing.List[
            CrawlerRegionDate.CrawlerRegionDateData]

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
                CrawlerRegionDate.from_json(x) for x in data["region_date"]]
        )


def slack_failure_percentage_statistics(
    total_statistics: CrawlerStatistics, failure_statistics: CrawlerStatistics,
) -> typing.Dict[str, typing.Any]:
    total_statistics_dict = attr.asdict(total_statistics)
    failure_statistics_dict = attr.asdict(failure_statistics)

    keys = tuple(total_statistics_dict.keys())

    result = dict()
    for key in keys:
        total_value = total_statistics_dict[key]
        failure_value = failure_statistics_dict[key]
        try:
            result[key] = (
                f"total: {total_value}\n"
                f"fail: {failure_value}\n"
                f"{100 *  failure_value / total_value}%"
            )
        except ZeroDivisionError:
            result[key] = (
                f"total: {total_value}\n"
                f"fail: {failure_value}\n"
            )
    return result
