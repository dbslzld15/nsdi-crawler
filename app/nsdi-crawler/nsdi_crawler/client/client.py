import functools
import json
import typing
import requests
from requests_toolbelt.sessions import BaseUrlSession
from tanker.utils.requests import apply_proxy
from tanker.utils.retryer import Retryer
from tanker.utils.retryer.strategy import ExponentialModulusBackoffStrategy
from nsdi_crawler.client.exc import NsdiClientResponseError
from .data import NsdiLandUsingInfoResponse, NsdiLandUsingInfo, NsdiRegion

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    " AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/84.0.4147.135 Safari/537.36"
)


class NsdiClient(object):
    def __init__(self, config: typing.Dict[str, typing.Any]) -> None:
        super().__init__()

        proxy = config.get("PROXY_HOST") or None
        # Header Settings
        self.session = BaseUrlSession("http://openapi.nsdi.go.kr/")
        self.session.headers.update({"User-Agent": USER_AGENT})

        if proxy:
            apply_proxy(self.session, proxy)

        self.retryer = Retryer(
            strategy_factory=(
                ExponentialModulusBackoffStrategy.create_factory(2, 10)
            ),
            should_retry=lambda e: isinstance(
                e, (requests.exceptions.ConnectionError,)
            ),
            default_max_trials=3,
        )

    def _handle_json_response(
        self, r: requests.Response
    ) -> typing.Dict[str, typing.Any]:
        r.raise_for_status()

        try:
            data = r.json()
            return data
        except (json.JSONDecodeError, ValueError):
            raise NsdiClientResponseError(r.status_code, r.text)

    def _handle_text_response(self, r: requests.Response) -> str:
        r.raise_for_status()

        try:
            r.json()
        except (json.JSONDecodeError, ValueError):
            return r.text
        else:
            raise NsdiClientResponseError(r.status_code, r.text)

    def init_page(
        self, prov_org: str, gubun: str, svc_se: str, svc_id: str
    ) -> requests.Response:
        # 세션에 쿠키와 세션정보를 넣어줌
        params1 = (
            ("provOrg", prov_org),
            ("gubun", gubun),
        )

        parmas = {
            "svcSe": svc_se,
            "svcId": svc_id,
        }

        response1 = self.session.get(
            "/nsdi/eios/OpenapiList.do", params=params1
        )
        self._handle_text_response(response1)

        response = self.session.get(
            "/nsdi/eios/ServiceDetail.do", params=parmas
        )
        self._handle_text_response(response)

        return response

    def fetch_land_using_info_table(
        self,
        svc_se: str,
        svc_id: str,
        start_date: str,
        end_date: str,
        extrc_se_search: str,
        prov_org: str,
        page_index: int,
    ) -> NsdiLandUsingInfoResponse:

        data = {
            "svcSe": svc_se,
            "svcId": svc_id,
            "pageIndex": "1",
            "provOrg": prov_org,
            "startDate": start_date,
            "endDate": end_date,
            "doArea": "",
            "svcNmSearch": "",
            "pageIndexSecond": "1",
        }

        if prov_org == "NIDO":
            data.update(
                {
                    "extrcSeSearch": extrc_se_search,
                }
            )

        if page_index:
            data.update({"pageIndexSecond": str(page_index)})

        response = self._handle_text_response(
            self.retryer.run(
                (
                    functools.partial(
                        self.session.post,
                        "/nsdi/eios/ServiceDetail.do",
                        data=data,
                    )
                )
            )
        )

        return NsdiLandUsingInfoResponse.from_html(response)

    def fetch_download_response(
        self,
        table_data: NsdiLandUsingInfo.NsdiTableData, prov_org: str
    ) -> requests.Response:
        data = {
            "opertSnDialog": table_data.opert_sn_dialog,
            "fileNmDialog": table_data.file_nm_dialog,
            "extrcScopeDialog": table_data.extrc_scope_dialog,
            "extrcSeDialog": table_data.extrc_se_dialog,
            "extrcDtDialog": table_data.extrc_dt_dialog,
            "svcIdDialog": table_data.svcld_dialog,
            "checkedValue": "",
            "downloadFileTy": "",
            "provOrg": prov_org,
        }

        response = self.retryer.run(
            functools.partial(
                self.session.post, "/nsdi/eios/fileDownload.do", data=data
            )
        )
        # 다운로드시에 response.iter_content를 사용해야하기때문에 이와 같이 설정하였습니다
        response.raise_for_status()

        return response

    def fetch_region_list(
        self, response: requests.Response
    ) -> typing.List[NsdiRegion]:
        self.session.headers.update({"Referer": response.url})

        response = self._handle_json_response(
            self.retryer.run(
                functools.partial(
                    self.session.get,
                    "/nsdi/eios/service/rest/AdmService/admCodeList.json",
                )
            )
        )

        region_list = response["admVOList"]["admVOList"]

        return [NsdiRegion.from_json(x) for x in region_list]

    def fetch_region_detail_list(
        self, adm_code: str
    ) -> typing.List[NsdiRegion]:

        params = {"admCode": adm_code}
        response = self._handle_json_response(
            self.retryer.run(
                functools.partial(
                    self.session.get,
                    "/nsdi/eios/service/rest/AdmService/admSiList.json",
                    params=params,
                )
            )
        )

        region_detail_list = response["admVOList"]["admVOList"]

        return [NsdiRegion.from_json(x) for x in region_detail_list]
