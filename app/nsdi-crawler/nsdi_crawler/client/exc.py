class NsdiClientError(Exception):
    pass


class NsdiClientResponseError(NsdiClientError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(status_code, message)


class NsdiClientParseError(NsdiClientError):
    pass
