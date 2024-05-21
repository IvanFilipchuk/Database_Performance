UPDATE passengers p
    JOIN (
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
    ) AS new_ratings ON p.id = new_ratings.id
    SET p.rating = new_ratings.new_rating;
