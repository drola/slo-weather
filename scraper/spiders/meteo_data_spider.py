import scrapy

from scraper.items import StationArchiveXml


class MeteoDataSpider(scrapy.Spider):
    name = "meteo_data"

    def __init__(self, meteosiId: str, *args, **kwargs):
        super(MeteoDataSpider, self).__init__(*args, **kwargs)
        self.start_urls = [
            f"https://meteo.arso.gov.si/uploads/probase/www/observ/surface/text/sl/recent/observationAms_{meteosiId}_history.xml"
        ]

    def parse(self, response):
        yield StationArchiveXml(
            meteosiId=response.xpath('//metData/domain_meteosiId/text()').get(),
            xml=response.text
        )
