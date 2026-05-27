CREATE SCHEMA IF NOT EXISTS carbon_data;

DROP TABLE IF EXISTS carbon_data.carbon_intensity_daily;

CREATE TABLE IF NOT EXISTS carbon_data.carbon_intensity_daily (
    regionid         INT NOT NULL,
    date_recorded    DATE NOT NULL,
    shortname        VARCHAR(100) NOT NULL,
    dno              VARCHAR(100) NOT NULL,
    intensity_avg    DECIMAL(10,2),
    index_mode       VARCHAR(50),
    fuel_biomass     DECIMAL(10,2),
    fuel_coal        DECIMAL(10,2),
    fuel_gas         DECIMAL(10,2),
    fuel_hydro       DECIMAL(10,2),
    fuel_imports     DECIMAL(10,2),
    fuel_nuclear     DECIMAL(10,2),
    fuel_other       DECIMAL(10,2),
    fuel_solar       DECIMAL(10,2),
    fuel_wind        DECIMAL(10,2),
    PRIMARY KEY (regionid, date_recorded)
);