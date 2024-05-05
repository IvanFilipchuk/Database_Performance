SELECT at.id, at.price, at.flight_time, at.class_of_ticket
FROM air_tickets AS at
         JOIN airport AS dep ON at.id_airport_of_departure = dep.id
         JOIN airport AS dest ON at.id_airport_of_destination = dest.id
WHERE dep.passenger_traffice > 5000 AND dest.passenger_traffice > 5000;
