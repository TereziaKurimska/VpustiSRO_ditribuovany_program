-- Vytvorenie databázy
CREATE DATABASE vpusti;
\c vpusti;

-- Tabuľka zamestnancov
CREATE TABLE Employee (
                          employee_id SERIAL PRIMARY KEY,
                          name VARCHAR(100) NOT NULL
);

-- Tabuľka lokácií
CREATE TABLE Location (
                          location_id SERIAL PRIMARY KEY,
                          name VARCHAR(100) NOT NULL
);

-- Tabuľka kariet
CREATE TABLE Card (
                      card_id SERIAL PRIMARY KEY,
                      employee_id INT REFERENCES Employee(employee_id) ON DELETE CASCADE
);

-- Tabuľka pre evidenciu prístupov
CREATE TABLE AccessLog (
                           log_id SERIAL PRIMARY KEY,
                           card_id INT REFERENCES Card(card_id) ON DELETE CASCADE,
                           location_id INT REFERENCES Location(location_id),
                           timestamp TIMESTAMP NOT NULL,
                           direction VARCHAR(3) CHECK (direction IN ('IN', 'OUT'))
);

-- Add a test employee
INSERT INTO Employee (name) VALUES ('John Doe');
INSERT INTO Employee (name) VALUES ('Jane Smith');
INSERT INTO Employee (name) VALUES ('Alice Johnson');



-- Add some locations
INSERT INTO Location (name) VALUES ('Main Entrance');
INSERT INTO Location (name) VALUES ('Side Door');



-- Add their card
INSERT INTO Card (employee_id) VALUES (1);
INSERT INTO Card (employee_id) VALUES (2);
INSERT INTO Card (employee_id) VALUES (3);

