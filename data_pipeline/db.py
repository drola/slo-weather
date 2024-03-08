import psycopg

dsn = "postgresql://slo_weather:slo_weather@localhost:16432/slo_weather"


def connect(autocommit=False):
    return psycopg.connect(dsn, autocommit=autocommit)
