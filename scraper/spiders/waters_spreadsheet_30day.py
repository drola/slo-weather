from .waters_spreadsheet_base import WatersSpreadsheetBaseSpider


class WatersSpreadsheet30daySpider(WatersSpreadsheetBaseSpider):
    name = "waters_30day"
    start_urls = ["https://www.arso.gov.si/vode/podatki/amp/Ht_30.html"]
