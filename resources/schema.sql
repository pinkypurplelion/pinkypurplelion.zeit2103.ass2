CREATE TABLE IF NOT EXISTS Olympics(
    ID INT NOT NULL AUTO_INCREMENT,
    year INT,
    season VARCHAR(7),
    city VARCHAR(23),
    PRIMARY KEY (ID),
    INDEX (year, season, city));

CREATE TABLE IF NOT EXISTS Events(
    ID INT NOT NULL AUTO_INCREMENT,
    sport VARCHAR(26),
    event VARCHAR(86),
    PRIMARY KEY (ID),
    INDEX (sport, event));

CREATE TABLE IF NOT EXISTS Athletes(
    ID INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(94),
    noc CHAR(3),
    gender CHAR(1),
    PRIMARY KEY (ID),
    INDEX (name, noc, gender));

CREATE TABLE IF NOT EXISTS Medals(
    ID INT NOT NULL AUTO_INCREMENT,
    olympicID INT, FOREIGN KEY (olympicID) REFERENCES Olympics(ID),
    eventID INT, FOREIGN KEY (eventID) REFERENCES Events(ID),
    athleteID INT, FOREIGN KEY (athleteID) REFERENCES Athletes(ID),
    medalColour VARCHAR(7),
    PRIMARY KEY (ID));