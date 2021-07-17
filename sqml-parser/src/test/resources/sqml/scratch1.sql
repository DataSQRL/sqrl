IMPORT wellmart.ord;

-- As entity
Customer := SELECT email, collect(cid) AS ids, collect(name, first_name + ' ' + last_name) AS names
FROM
    (SELECT * FROM customer_mdm
    UNION ALL
    SELECT * FROM customer_db)
GROUP BY email;
-- As function
Customer.name := util.longest(names);
-- As relationship to entity
Customer.orders := INNER JOIN ord ON ord.customer_id IN (Customer.ids);
-- As Query
Customer._recent_items := SELECT total, _parent.time, product.category AS category FROM "@.orders.entries" WHERE _parent.time > now() - INTERVAL '6' MONTH;

--CREATE RELATIONSHIP ords FROM Customer JOIN ord ON ord.customer_id IN ("Customer.ids") INVERSE customer;

--ALTER TABLE Customer_mdm RENAME TO _Customer_mdm;

--Customer.favorite_categories := SELECT category, sum(weight) AS total_weight FROM "@._recent_items" GROUP BY category ORDER BY total_weight DESC;

--CREATE SUBSCRIPTION "send_savings_email" ON ADD AS
--SELECT email, name, total_savings FROM Customer WHERE total_savings > 100;

--Customer.savings_by_time(@period duration) := sum(ords[@.time > (NOW() - @period)].entries.discount);

Product.orderVolumeByDay := SELECT time.roundToDay(_parent.time) AS day, sum(quantity) AS quantity FROM "@.purchases" GROUP BY day ORDER BY day DESC;

LEARN RELATIONSHIP _ProductRecommendations ON _PurchaseRelationships PREDICT DISTINCT customer, product WHERE confidence>0.7;

LEARN FORECAST Product.orderVolumeForecast ON (SELECT day, quantity FROM "@.orderVolumeByDay" ORDER BY day DESC) PREDICT quantity LIMIT 20;

Ord.entries.total := quantity * unit_price - discount;

SELECT name, collect(orders.id) FROM Customer;

query {
 Customer {
   email
   name
   orders {
     id
   }
 }
}

query {
 Ord {
   id
   entries {
     total
   }
 }
}