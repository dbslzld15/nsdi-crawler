"""
config
======

"""
import tanker.config
import typing
from tanker.config import fields

SCHEMA = {
    #: Running environment
    "ENVIRONMENT": fields.OneOfField(
        {"local", "test", "development", "production", }, default="local",
    ),
    #: Debug
    "DEBUG": fields.BooleanField(optional=True),
    #: Running environment
    "PROXY_HOST": fields.StringField(optional=True),
    #: Search S3 nsdi data type : 변동데이터, 전체데이터
    "DATA_TYPE": fields.StringField(optional=True),
    #: Crawler Download Mode : ON은 실제 파일 저장 OFF는 리소스 파일
    "DOWNLOAD": fields.StringField(optional=True),
    #: AWS sepecific access key id value
    "AWS_ACCESS_KEY_ID": fields.StringField(optional=True),
    #: AWS sepecific secret access key value
    "AWS_SECRET_ACCESS_KEY": fields.StringField(optional=True),
    #: AWS sepecific region name value
    "AWS_REGION_NAME": fields.StringField(optional=True),
    #: AWS sepecific endpoint url value
    "AWS_ENDPOINT_URL": fields.StringField(optional=True),
    #: AWS sepecific endpoint url value
    "AWS_S3_BUCKET_NAME": fields.StringField(optional=True),
    #: Slack Info
    "SLACK_API_TOKEN": fields.StringField(optional=True),
    "SLACK_CHANNEL": fields.StringField(optional=True),
    #: Sentry DSN
    'SENTRY_DSN': fields.StringField(optional=True),
}


def load() -> typing.Dict[str, typing.Union[object, str]]:
    config = tanker.config.load_from_env(prefix="CRAWLER_", schema=SCHEMA)
    config.setdefault("DEBUG", config["ENVIRONMENT"] in {"local", "test"})
    return config
