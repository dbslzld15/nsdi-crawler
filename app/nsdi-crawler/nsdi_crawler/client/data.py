import typing
from abc import abstractmethod, ABCMeta
import attr
import re
import bs4


class NsdiHtmlData(metaclass=ABCMeta):
    @abstractmethod
    def to_html(self) -> str:
        pass


class NsdiIndexData(NsdiHtmlData):
    @abstractmethod
    def pk(self) -> str:
        pass


@attr.s(frozen=True)
class NsdiLandUsingInfo(NsdiHtmlData):
    @attr.s(frozen=True)
    class NsdiTableData(NsdiHtmlData):
        # data : F014
        # description : ?
        svcld_dialog: str = attr.ib()
        # data : CH
        # description : ?
        extrc_se_dialog: str = attr.ib()
        # data : 20200908
        # description : 다운받은 현재 날짜
        extrc_dt_dialog: str = attr.ib()
        # data : 00
        # description : ?
        extrc_scope_dialog: str = attr.ib()
        # data : CH_00_D155_20200908.zip
        # description : 파일 이름
        file_nm_dialog: str = attr.ib()
        # data : 2448
        # description : ?
        opert_sn_dialog: str = attr.ib()
        # 원본 데이터
        raw_data: str = attr.ib()

        @classmethod
        def from_html(
                cls,
                data: bs4.element.Tag
        ) -> "NsdiLandUsingInfo.NsdiTableData":
            value = data['onclick']  # onclick 속성 가져오기
            data = re.findall(r"['\"](.*?)['\"]", value)
            svcld_dialog = data[0]
            extrc_dt_dialog = data[1]
            extrc_se_dialog = data[2]
            extrc_scope_dialog = data[3]
            file_nm_dialog = data[4]
            opert_sn_dialog = data[5]
            raw_data = str(data)

            return cls(
                svcld_dialog=svcld_dialog,
                extrc_se_dialog=extrc_se_dialog,
                extrc_scope_dialog=extrc_scope_dialog,
                extrc_dt_dialog=extrc_dt_dialog,
                file_nm_dialog=file_nm_dialog,
                opert_sn_dialog=opert_sn_dialog,
                raw_data=raw_data
            )

        def to_html(self) -> str:
            return self.raw_data

    # data : 전체데이터, 변동데이터
    # description : 구분
    data_type: str = attr.ib()
    # data : 전국, 서울특별시 등
    # description : 구분
    city_type: str = attr.ib()
    # data : 토지이용계획정보
    # description : 데이터셋명
    name_type: str = attr.ib()
    # data : 2020-09-09
    # description : 기준일자
    base_date: str = attr.ib()
    # data : 3,527 KB
    # description : 파일크기
    file_size: typing.Optional[str] = attr.ib()
    # data : 변동데이터, 전체데이터
    # description : 구분
    table_data: NsdiTableData = attr.ib()
    #: Raw tr data
    raw_data: str = attr.ib()

    @classmethod
    def from_html(cls, tr: bs4.element.Tag
                  ) -> "NsdiLandUsingInfo":
        td_list = tr.select("td")
        data_type = td_list[0].text.strip()
        city_type = td_list[1].text.strip()
        name_type = td_list[2].text.strip()
        base_date = td_list[3].text.strip()
        file_size = td_list[4].text.strip()
        button_value = td_list[5].select("button")[0]
        table_data = NsdiLandUsingInfo.NsdiTableData.from_html(button_value)

        return cls(
            data_type=data_type,
            city_type=city_type,
            name_type=name_type,
            base_date=base_date,
            file_size=file_size,
            table_data=table_data,
            raw_data=str(tr)
        )

    def to_html(self) -> str:
        return self.raw_data


@attr.s(frozen=True)
class NsdiLandUsingInfoResponse(NsdiHtmlData):
    # data: []
    # description: 토지이용정보
    land_using_info: typing.List[NsdiLandUsingInfo] = attr.ib()
    # data: 22
    # description: 총 페이지 수
    total_page: int = attr.ib()
    #: Raw 페이지 네이션
    raw_data: str = attr.ib()

    @classmethod
    def from_html(cls, data: str) -> "NsdiLandUsingInfoResponse":
        soup = bs4.BeautifulSoup(data, 'lxml')
        tbody = soup.select("#fileListForm > table > tbody")
        tr_list = tbody[0].select("tr")

        total_page_data = soup.find('button', attrs={'class': 'btn-last'})
        total_page_value = total_page_data['onclick']
        total_page = int(re.findall(r"\d+", total_page_value)[0])

        return cls(
            land_using_info=[
                NsdiLandUsingInfo.from_html(x) for x in tr_list],
            total_page=total_page,
            raw_data=str(data)
        )

    def to_html(self) -> str:
        return self.raw_data


@attr.s(frozen=True)
class NsdiRegion(object):
    # data: 서울특별시
    # description: 시도
    lowest_adm_code_nm: str = attr.ib()
    # data: 11
    # description: 지역코드(?)
    adm_code: str = attr.ib()
    # data: 서울특별시
    # description: 시도
    adm_code_nm: str = attr.ib()
    # data: null
    # description: ?
    lnm: typing.Optional[str] = attr.ib()
    # data: null
    # description: ?
    mnnm: typing.Optional[str] = attr.ib()
    # data: null
    # description: ?
    slno: typing.Optional[str] = attr.ib()
    # data: null
    # description: ?
    ldCpsgCode: typing.Optional[str] = attr.ib()
    # data: null
    # description: ?
    ldEmdLiCode: typing.Optional[str] = attr.ib()
    # data: null
    # description: ?
    regstrSeCode: typing.Optional[str] = attr.ib()
    # data: null
    # description: ?
    pnu: typing.Optional[str] = attr.ib()

    @classmethod
    def from_json(cls, data: typing.Dict[str, typing.Any]
                  ) -> "NsdiRegion":
        return cls(
            lowest_adm_code_nm=data.get("lowestAdmCodeNm"),
            adm_code=data.get("admCode"),
            adm_code_nm=data.get("admCodeNm").strip(),
            lnm=data.get("lnm"),
            mnnm=data.get("mnnm"),
            slno=data.get("slno"),
            ldEmdLiCode=data.get("ldEmdLiCode"),
            ldCpsgCode=data.get("ldCpsgCode"),
            regstrSeCode=data.get("regstrSeCode"),
            pnu=data.get("pnu")
        )
