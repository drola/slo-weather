CREATE TABLE IF NOT EXISTS weather_datapoints
(
    id                          SERIAL
        constraint weather_datapoints_pk
            primary key,
    station_arso_code           VARCHAR(255) NOT NULL
        constraint fk_weather_datapoints_station_arso_code
            references stations(arso_code),
    sunrise                     TIMESTAMP,
    sunset                      TIMESTAMP,
    interval_start              TIMESTAMP,
    interval_end                TIMESTAMP,
    temperature_dew_point       REAL,
    temperature_air_avg         REAL,
    temperature_air_max         REAL,
    temperature_air_min         REAL,
    humidity_relative_avg       REAL,
    wind_direction_avg          REAL,
    wind_direction_max_gust     REAL,
    wind_speed_avg              REAL,
    wind_speed_max              REAL,
    pressure_mean_sea_level_avg REAL,
    pressure_surface_level_avg  REAL,
    precipitation_sum_10min     REAL,
    precipitation_sum_1h        REAL,
    precipitation_sum_24h       REAL,
    snow_cover_height           REAL,
    sun_radiation_global_avg    REAL,
    sun_radiation_diffuse_avg   REAL,
    visibility                  REAL
);

alter table weather_datapoints
    add constraint weather_datapoints_pk2
        unique (station_arso_code, interval_start, interval_end);

