def uri_params(params, spider):
    """See https://docs.scrapy.org/en/latest/topics/feed-exports.html#feed-uri-params"""
    return {**params, "spider_name": spider.name}
