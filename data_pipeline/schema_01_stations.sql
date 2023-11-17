create table stations
(
    id                      serial
        constraint stations_pk
            primary key,
    arso_code               varchar(255) not null
        constraint stations_pk2
            unique,
    coordinates             point   not null,
    altitude                float not null,
    name                    text    not null,
    name_short              text    not null,
    name_long               text    not null
)