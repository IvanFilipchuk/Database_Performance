UPDATE air_tickets
SET price = price * 1.1
WHERE class_of_ticket = 'first class';
UPDATE pilots SET pilot_rating = CASE WHEN years_practice >= 10 THEN pilot_rating * 1.1 WHEN years_practice >= 5 THEN pilot_rating * 1.05 ELSE pilot_rating END WHERE years_practice >= 5;
