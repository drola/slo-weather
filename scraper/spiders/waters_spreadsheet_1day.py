from .waters_spreadsheet_base import WatersSpreadsheetBaseSpider


class WatersSpreadsheet1daySpider(WatersSpreadsheetBaseSpider):
    name = "waters_1day"
    start_urls = ["https://www.arso.gov.si/vode/podatki/amp/Ht_1.html"]
