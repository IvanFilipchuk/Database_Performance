-- Create the database
CREATE DATABASE airport_tickets;
CREATE TABLE airport (
                         id SERIAL PRIMARY KEY,
                         airport_code VARCHAR(100),
                         country VARCHAR(100),
                         passenger_traffice INT
);

CREATE TABLE air_tickets (
                             id SERIAL PRIMARY KEY,
                             id_airport_of_departure INT,
                             id_airport_of_destination INT,
                             time_of_leave VARCHAR(50),
                             time_of_arrival VARCHAR(50),
                             flight_time VARCHAR(50),
                             price NUMERIC,
                             class_of_ticket VARCHAR(50),
                            FOREIGN KEY (id_airport_of_departure) REFERENCES airport(id),
                            FOREIGN KEY (id_airport_of_destination) REFERENCES airport(id)
);

-- Create table airport

CREATE TABLE  pilots (
                                      id SERIAL PRIMARY KEY,
                                      first_name VARCHAR(50),
                                      last_name VARCHAR(50),
                                      gender VARCHAR(50),
                                      age DECIMAL(3,1),
                                      pilot_rating FLOAT,
                                      years_practice DECIMAL(3,1)
);

CREATE TABLE passengers (
                                          id SERIAL PRIMARY KEY,
                                          first_name VARCHAR(50),
                                          last_name VARCHAR(50),
                                          email VARCHAR(100),
                                          gender VARCHAR(50),
                                          rating FLOAT,
                                          pilots_id INT,
                                          FOREIGN KEY (pilots_id) REFERENCES pilots(id)
);
CREATE INDEX idx_air_tickets_id_airport_of_departure ON air_tickets (id_airport_of_departure);
CREATE INDEX idx_air_tickets_id_airport_of_destination ON air_tickets (id_airport_of_destination);
CREATE INDEX idx_passengers_pilots_id ON passengers (pilots_id);