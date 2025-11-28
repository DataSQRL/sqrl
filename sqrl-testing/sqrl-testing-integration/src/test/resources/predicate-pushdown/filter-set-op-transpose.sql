CREATE TEMPORARY VIEW T AS
    SELECT * FROM (
        VALUES (1, 'DE', 5),
               (2, 'US', 50),
               (3, 'DE', 500)
    ) AS V(a, country, v);

SELECT a, v FROM (SELECT a, v FROM T UNION ALL SELECT a, v FROM T ) z WHERE a > 1;
