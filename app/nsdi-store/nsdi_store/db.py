import sqlalchemy as sa
import typing
from sqlalchemy import orm


def create_session_factory(
    config: typing.Dict[str, typing.Any]
) -> orm.session:
    db_uri = config["SQLALCHEMY_DATABASE_URI"]
    db_engine = sa.create_engine(db_uri)
    session_factory = orm.sessionmaker(bind=db_engine)
    return session_factory
