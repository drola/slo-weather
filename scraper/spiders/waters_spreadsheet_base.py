import scrapy
from scrapy import Request
from scrapy.http import Response

from ..waters_parsing_util import parse_page_with_table


class WatersSpreadsheetBaseSpider(scrapy.Spider):
    allowed_domains = ["www.arso.gov.si"]

    def parse(self, response: Response):
        for area_href in response.css(
                'map[name="Zemljevid"] area::attr(href)'
        ).getall():
            yield Request(response.urljoin(area_href), self.parse_spreadsheet)

    def parse_spreadsheet(self, response: Response):
        location = response.css(".vsebina > h1::text").get()
        table = response.css("table.podatki")
        return parse_page_with_table(table, location, response.text)
