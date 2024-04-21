-- Create the database
CREATE DATABASE airport_tickets;


CREATE TABLE air_tickets (
                             id SERIAL PRIMARY KEY,
                             id_airport_of_departure INT,
                             id_airport_of_destination INT,
                             time_of_leave TIMESTAMP,
                             time_of_arrival TIMESTAMP,
                             flight_time INTERVAL,
                             price NUMERIC,
                             class_of_ticket VARCHAR(50)
);

-- Create table airport
CREATE TABLE airport (
                         id SERIAL PRIMARY KEY,
                         airport_code VARCHAR(100),
                         country VARCHAR(100),
                         passenger_traffice INT
);
