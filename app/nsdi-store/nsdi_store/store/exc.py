class NsdiStoreError(Exception):
    pass


class NsdiStoreS3NotFound(NsdiStoreError):
    pass


class NsdiStoreCrawlerLogNotFound(NsdiStoreError):
    pass


class NsdiStoreRegionNotFound(NsdiStoreError):
    pass
