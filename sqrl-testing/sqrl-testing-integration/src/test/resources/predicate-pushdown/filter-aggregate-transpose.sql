CREATE TEMPORARY VIEW T AS
    SELECT * FROM (
        VALUES (1, 'DE', 5),
               (2, 'US', 50),
               (3, 'DE', 500)
    ) AS V(a, country, v);

SELECT country, COUNT(*) AS c FROM T GROUP BY country HAVING country = 'DE';
