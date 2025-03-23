package si.drola.adventofcode2024;

import java.time.Instant;

/**
 * class MetDataXmlHandler(ContentHandler):
 * def __init__(self):
 * self.datapoints = []
 * self.currentDatapoint = {}
 * self.currentElement = ""
 * self.currentCharacters = ""
 * <p>
 * def startElement(self, name, attrs):
 * self.currentElement = name
 * self.currentCharacters = ""
 * if name == "metData":
 * self.currentDatapoint = {}
 * <p>
 * def characters(self, content):
 * self.currentCharacters += content
 * <p>
 * def endElement(self, name):
 * if name == "domain_meteosiId":
 * self.currentDatapoint["station_arso_code"] = self.currentCharacters.strip("_")
 * elif name == "sunrise":
 * self.currentDatapoint["sunrise"] = parse_arso_datetime(self.currentCharacters)
 * elif name == "sunset":
 * self.currentDatapoint["sunset"] = parse_arso_datetime(self.currentCharacters)
 * elif name == "validStart":
 * self.currentDatapoint["interval_start"] = parse_arso_datetime(self.currentCharacters)
 * elif name == "validEnd":
 * self.currentDatapoint["interval_end"] = parse_arso_datetime(self.currentCharacters)
 * elif name == "td":
 * self.currentDatapoint["temperature_dew_point"] = float_or_none(self.currentCharacters)
 * elif name == "tavg":
 * self.currentDatapoint["temperature_air_avg"] = float_or_none(self.currentCharacters)
 * elif name =="tx":
 * self.currentDatapoint["temperature_air_max"] = float_or_none(self.currentCharacters)
 * elif name == "tn":
 * self.currentDatapoint["temperature_air_min"] = float_or_none(self.currentCharacters)
 * elif name =="rhavg":
 * self.currentDatapoint["humidity_relative_avg"] = float_or_none(self.currentCharacters)
 * elif name =="ddavg_val":
 * self.currentDatapoint["wind_direction_avg"] = float_or_none(self.currentCharacters)
 * elif name =="ddmax_val":
 * self.currentDatapoint["wind_direction_max_gust"] = float_or_none(self.currentCharacters)
 * elif name =="ffavg_val":
 * self.currentDatapoint["wind_speed_avg"] = float_or_none(self.currentCharacters)
 * elif name =="ffmax_val":
 * self.currentDatapoint["wind_speed_max"] = float_or_none(self.currentCharacters)
 * elif name =="mslavg":
 * self.currentDatapoint["pressure_mean_sea_level_avg"] = float_or_none(self.currentCharacters)
 * elif name =="pavg":
 * self.currentDatapoint["pressure_surface_level_avg"] = float_or_none(self.currentCharacters)
 * elif name=="rr_val":
 * self.currentDatapoint["precipitation_sum_10min"] = float_or_none(self.currentCharacters)
 * elif name=="tp_1h_acc":
 * self.currentDatapoint["precipitation_sum_1h"] = float_or_none(self.currentCharacters)
 * elif name=="tp_24h_acc":
 * self.currentDatapoint["precipitation_sum_24h"] = float_or_none(self.currentCharacters)
 * elif name=="snow":
 * self.currentDatapoint["snow_cover_height"] = float_or_none(self.currentCharacters)
 * elif name=="gSunRadavg":
 * self.currentDatapoint["sun_radiation_global_avg"] = float_or_none(self.currentCharacters)
 * elif name=="diffSunRadavg":
 * self.currentDatapoint["sun_radiation_diffuse_avg"] = float_or_none(self.currentCharacters)
 * elif name=="vis_val":
 * self.currentDatapoint["visibility"] = float_or_none(self.currentCharacters)
 * elif name == "metData":
 * self.datapoints.append(self.currentDatapoint)
 * # self.datapoints.append(Datapoint.model_validate(self.currentDatapoint))
 */

public record MetDataPoint(
        String stationArsoCode,
        Instant intervalEnd,
        float precipitationSum10min
) {
}
