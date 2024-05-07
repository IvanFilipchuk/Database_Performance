BEGIN;

UPDATE air_tickets AS at
SET time_of_leave = at.time_of_leave + INTERVAL '1 day',
    time_of_arrival = at.time_of_arrival + INTERVAL '1 day'
WHERE at.id_airport_of_departure IN (SELECT id FROM airport WHERE country = 'China')
  AND at.id_airport_of_destination IN (SELECT id FROM airport WHERE country = 'China');

UPDATE airport
SET passenger_traffice = passenger_traffice + 1000
WHERE country = 'China';

COMMIT;
