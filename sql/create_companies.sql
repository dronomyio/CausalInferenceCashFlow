CREATE DATABASE IF NOT EXISTS SPYStocks;
USE SPYStocks;
--drop tables
drop table Companies;
drop table StockPrices;
 -- Table for storing company data
CREATE TABLE Companies (
 id INT AUTO_INCREMENT PRIMARY KEY,
 symbol VARCHAR(10) NOT NULL, -- Stock symbol
 security VARCHAR(255) NOT NULL, -- Security name
 gics_sector VARCHAR(100) NOT NULL, -- GICS sector
 gics_sub_industry VARCHAR(255) NOT NULL, -- GICS sub-industry
 headquarters_location VARCHAR(255) NOT NULL -- Headquarters location
);
 -- Index for faster lookups by symbol
CREATE INDEX idx_symbol ON Companies(symbol);

USE SPYStocks;
CREATE TABLE StockPrices (
    Ticker VARCHAR(10) NOT NULL,
    Date DATE NOT NULL,
    Price DECIMAL(10,2),
    AdjClose DECIMAL(10,2),
    Close DECIMAL(10,2),
    High DECIMAL(10,2),
    Low DECIMAL(10,2),
    Open DECIMAL(10,2),
    Volume BIGINT,
    PRIMARY KEY (Ticker, Date)
);
