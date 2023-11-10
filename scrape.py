from scrapy import signals
from scrapy.utils.reactor import install_reactor

install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings

import scraper.spiders.stations_spider

settings = get_project_settings()
configure_logging(settings)
runner = CrawlerRunner(settings)


@defer.inlineCallbacks
def crawl():
    items = []

    stations_crawler = runner.create_crawler(scraper.spiders.stations_spider.StationsSpider)
    stations_crawler.signals.connect(lambda item, response, spider: items.append(item), signal=signals.item_scraped)
    print("BEFORE")
    c = yield stations_crawler.crawl()
    print("AFTER")
    print(items)
    #
    reactor.stop()


crawl()
reactor.run()  # the script will block here until the last crawl call is finished
