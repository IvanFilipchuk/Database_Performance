SELECT at.*, dep.country AS departure_country, dest.country AS destination_country
FROM air_tickets AS at
         JOIN airport AS dep ON at.id_airport_of_departure = dep.id
         JOIN airport AS dest ON at.id_airport_of_destination = dest.id;
SELECT pilots.*, COUNT(passengers.id) AS passenger_count FROM pilots LEFT JOIN passengers ON pilots.id = passengers.pilots_id GROUP BY pilots.id;
