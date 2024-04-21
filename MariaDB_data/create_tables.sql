
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

