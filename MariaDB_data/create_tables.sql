
CREATE TABLE IF NOT EXISTS pilots (
                                      id INT AUTO_INCREMENT PRIMARY KEY,
                                      first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(50),
    age DECIMAL(3,1),
    pilot_rating DECIMAL(3,1),
    years_practice DECIMAL(3,1)
    );

CREATE TABLE IF NOT EXISTS passengers (
                                          id INT AUTO_INCREMENT PRIMARY KEY,
                                          first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    gender VARCHAR(50),
    rating INT,
    pilots_id INT,
    FOREIGN KEY (pilots_id) REFERENCES pilots(id)
    );

CREATE TABLE IF NOT EXISTS airport (
                          id INT AUTO_INCREMENT PRIMARY KEY,
                         airport_code VARCHAR(100),
                         country VARCHAR(100),
                         passenger_traffice INT
);

CREATE TABLE IF NOT EXISTS air_tickets (
                             id INT AUTO_INCREMENT PRIMARY KEY,
                             id_airport_of_departure INT,
                             id_airport_of_destination INT,
                             time_of_leave VARCHAR(100),
                             time_of_arrival VARCHAR(100),
                             flight_time VARCHAR(50),
                             price NUMERIC,
                             class_of_ticket VARCHAR(50),
                             FOREIGN KEY (id_airport_of_departure) REFERENCES airport(id),
                             FOREIGN KEY (id_airport_of_destination) REFERENCES airport(id)
);


