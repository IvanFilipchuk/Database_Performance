UPDATE air_tickets a
    JOIN (
    SELECT at.id,
    CASE
    WHEN dep.country = 'China' THEN at.price * 1.2
    WHEN dest.country = 'Russia' THEN at.price * 1.15
    ELSE at.price
    END AS new_price
    FROM air_tickets at
    JOIN airport dep ON at.id_airport_of_departure = dep.id
    JOIN airport dest ON at.id_airport_of_destination = dest.id
    ) AS new_prices ON a.id = new_prices.id
    SET a.price = new_prices.new_price;