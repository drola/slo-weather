import scrapy

from scraper.items import Station, Point, StationArchiveXml


class StationsSpider(scrapy.Spider):
    name = "stations"

    def start_requests(self):
        manned_stations_url = 'https://meteo.arso.gov.si/uploads/probase/www/observ/surface/text/sl/observation_si_latest.xml'
        automatic_stations_url = 'https://meteo.arso.gov.si/uploads/probase/www/observ/surface/text/sl/observationAms_si_latest.xml'
        urls = [
            #manned_stations_url,
            automatic_stations_url,
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_stations_list)

    def parse_stations_list(self, response):
        for station in response.xpath('//metData'):
            station = Station(
                meteosiId=station.xpath('.//domain_meteosiId/text()').get().strip('_'),
                countryIsoCode2=station.xpath('.//domain_countryIsoCode2/text()').get(),
                coordinates=Point(
                    lon=float(station.xpath('.//domain_lon/text()').get()),
                    lat=float(station.xpath('.//domain_lat/text()').get()),
                ),
                altitude=float(station.xpath('.//domain_altitude/text()').get()),
                title=station.xpath('.//domain_title/text()').get(),
                longTitle=station.xpath('.//domain_longTitle/text()').get(),
                shortTitle=station.xpath('.//domain_shortTitle/text()').get()
            )
            yield station
            yield scrapy.Request(
                url=f"https://meteo.arso.gov.si/uploads/probase/www/observ/surface/text/sl/recent/observationAms_{station.meteosiId}_history.xml",
                callback=self.parse_meteo_data
            )

    def parse_meteo_data(self, response):
        yield StationArchiveXml(
            meteosiId=response.xpath('//metData/domain_meteosiId/text()').get(),
            xml=response.text
        )
