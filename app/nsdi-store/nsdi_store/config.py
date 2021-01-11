"""
config
======

"""
import typing

import tanker.config
from tanker.config import fields

SCHEMA = {
    #: Running environment
    'ENVIRONMENT': fields.OneOfField({
        'local',
        'test',
        'development',
        'production',
    }, default='local'),
    #: Debug
    'DEBUG': fields.BooleanField(optional=True),
    #: SQLAlchemy URI
    'SQLALCHEMY_DATABASE_URI': fields.StringField(optional=True),
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
    'SLACK_API_TOKEN': fields.StringField(optional=True),
    'SLACK_CHANNEL': fields.StringField(optional=True),
    #: Sentry DSN
    'SENTRY_DSN': fields.StringField(optional=True),
    # Store log id
    'CRAWLER_LOG_ID': fields.StringField(optional=True, default=None),
    # 시, 도 지역
    'REGION_REGEX_LEVEL_1': fields.StringField(optional=False),
    # 시, 군, 구 지역
    'REGION_REGEX_LEVEL_2': fields.StringField(optional=False),
}


def load() -> typing.Dict[str, typing.Any]:
    config = tanker.config.load_from_env(prefix='STORE_', schema=SCHEMA)
    config.setdefault('DEBUG', config['ENVIRONMENT'] in {'local', 'test'})

    return config
