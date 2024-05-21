UPDATE air_tickets AS a
SET price = new_price
FROM (
         SELECT at.id,
                CASE
                    WHEN dep.country = 'China' THEN at.price * 1.2
                    WHEN dest.country = 'Brazil' THEN at.price * 1.15
                    ELSE at.price
                    END AS new_price
         FROM air_tickets at
                  JOIN airport dep ON at.id_airport_of_departure = dep.id
                  JOIN airport dest ON at.id_airport_of_destination = dest.id
     ) AS new_prices
WHERE a.id = new_prices.id;
UPDATE passengers p
SET rating = new_ratings.new_rating
FROM (
         SELECT ps.id,
                CASE
                    WHEN pl.years_practice > 15 THEN ps.rating + 4
                    WHEN pl.gender = 'Female' AND pl.years_practice > 10 THEN ps.rating + 3
                    WHEN ps.gender = 'Genderfluid' THEN ps.rating - 1
                    WHEN pl.pilot_rating > 9.0 THEN ps.rating + 2
                    ELSE ps.rating + 1
                    END AS new_rating
         FROM passengers ps
                  JOIN pilots pl ON ps.pilots_id = pl.id
     ) AS new_ratings
WHERE p.id = new_ratings.id;

