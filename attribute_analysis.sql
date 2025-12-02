WITH 
non_archived_retailer AS (
    SELECT DISTINCT retailer_id, retailer_name
    FROM config_account
    WHERE archived = 0
),
latest_catalog AS (
    SELECT c.*
    FROM product_catalog c
    JOIN config_dataset_data_expiration AS ex ON ex.dataset_id = c.dataset_id
    JOIN config_dataset_info AS i ON i.dataset_id = c.dataset_id 
    JOIN non_archived_retailer AS nar ON c.retailer_id = nar.retailer_id
    WHERE c.update_time >= ex.cutoff_time
        AND i.archive_time IS NULL
),
columns AS (
    SELECT
        retailer_id,
        dataset_id,
        id,
        object_construct(
            /* 'id', id, */
            /* 'description', description, */
            /* 'image_link', image_link, */
            /* 'item_group_id', item_group_id, */
            /* 'link', link, */
            /* 'price', price, */
            'product_type', product_type,
            /* 'title', title, */
            /* 'additional_image_link', additional_image_link, */
            'adult', adult,
            'age_group', age_group,
            'availability', availability,
            /* 'availability_date', availability_date, */
            'brand', brand,
            'color', color,
            'condition', CONDITION,
            'energy_efficiency_class', energy_efficiency_class,
            'expiration_date', expiration_date,
            'gender', gender,
            'is_bundle', is_bundle,
            'loyalty_points', loyalty_points,
            'material', material,
            'mobile_link', mobile_link,
            'mpn', mpn,
            'multipack', multipack,
            'pattern', pattern,
            'promotion_id', promotion_id,
            'sale_price', sale_price,
            'sale_price_effective_date_begin', sale_price_effective_date_begin,
            'sale_price_effective_date_end', sale_price_effective_date_end,
            'shipping', shipping,
            'shipping_height', shipping_height,
            'shipping_label', shipping_label,
            'shipping_length', shipping_length,
            'shipping_width', shipping_width,
            'shipping_weight', shipping_weight,
            'size', size,
            'size_type', size_type,
            'tax', tax
        ) AS src
    FROM latest_catalog
    UNION ALL
    SELECT
        retailer_id,
        dataset_id,
        id,
        custom AS src
    FROM latest_catalog
),
column_values AS (
  SELECT c.retailer_id AS retailer_id, c.dataset_id AS dataset_id, field.key AS column_name, field.value AS column_value, count(*) AS k
  FROM columns c,
     lateral flatten(input => c.src, mode => 'object') field 
/*
WHERE column_name NOT IN (
    'activity',
    'averagerating_number1', 
    'category',
    'categoryid',
    'categoryidlevel0',
    'categoryidlevel1',
    'categoryidlevel2',
    'categoryidlevel3',
    'categoryidlevel4',
    'categoryidlevel5',
    'categorynamelevel0',
    'categorynamelevel1',
    'categorynamelevel2',
    'categorynamelevel3',
    'categorynamelevel4',
    'categorynamelevel5',
    'listprice_number4',
    'maxlistprice',
    'maxmapprice',
    'maxofferprice',
    'maxpercentoff',
    'minmapprice',
    'minofferprice',
    'minpercentoff',
    'offerprice_number3', 
    'productid',
    'producttype',
    'quantity',
    'reviewcount_number2',
    'storeid',
    'storeno', 
    'wcsidentifier'
    )
*/
  GROUP BY 1, 2, 3, 4
),
guesses AS (
    SELECT 
        retailer_id, dataset_id, column_name, 
        /* column_value, */
        CASE 
            WHEN column_value LIKE '%, and%' THEN 'has-comma-space-and'
            WHEN column_value LIKE '%, %' THEN 'has-comma-space'
            WHEN column_value LIKE '%,%' THEN 'has-comma'
            ELSE 'no-comma' 
        END type_guess,
        count(*) k
    FROM column_values
    GROUP BY 1, 2, 3, 4
)
SELECT nar.retailer_id, nar.retailer_name, g.dataset_id, c.dataset_name, g.column_name, c.column_type, g.type_guess,g.k
FROM guesses AS g
JOIN non_archived_retailer AS nar ON g.retailer_id = nar.retailer_id
LEFT JOIN developer.jjp_config_dataset_column c ON g.retailer_id = c.retailer_id AND g.dataset_id = c.dataset_id AND g.column_name = c.column_name

WHERE 
g.retailer_id = 1316 -- Academy Sports
-- g.retailer_id = 1170 -- Landmark
-- g.retailer_id = 697 -- Bathrooms.com / City Plumbing
-- g.retailer_id = 1383 AND g.dataset_id=65097 -- autozone2
-- g.retailer_id = 1393 -- Dicks Sporting Goods
-- g.retailer_id = 1446 -- HomeDepot CA
-- and g.dataset_id = 81892
-- g.retailer_id  = 1368 -- Advance Auto
-- g.retailer_id = 1612 -- Peter Alexander
-- g.retailer_id=131 -- cornerstone, garnet hill
-- g.retailer_id=136 -- cornerstone, ballard designs
-- g.retailer_id=137 -- cornerstone, frontgate
-- g.retailer_id=1382 -- athome
-- g.retailer_id=1397 -- GE Appliances
-- g.retailer_id=1211 -- Airgas
-- g.retailer_id=1205 -- dune
-- g.retailer_id=249 -- J Crew
-- g.retailer_id=1475 -- Randy's
-- g.retailer_id=138 -- cornerstone, grandin
-- AND g.dataset_id = 80987 -- dickssportinggoods
-- g.retailer_id = 1365 -- follet
-- g.retailer_id = 1491 
-- AND g.dataset_id=74839 -- guitar-center
-- g.retailer_id = 1401 -- imperialsupplies
-- and g.dataset_id=48944
-- g.retailer_id = 1310 -- parts-town  
-- g.retailer_id = 858 -- adidas
-- g.retailer_id=745

ORDER BY retailer_shortname, column_name, dataset_name, k DESC
;

/*
name, type, actual
category, string, string
category1, string, string
category2, multstring, string?
categorylevel2, multstring, string
categorylevel3, multstring, string
categorylevel4, multstring, string
categorylevel5, multstring, string
parttype, multistring, string
ratingrange, multistring, string (possible values: null, "5")
*/

-- DSG eyeball check

-- age csv
-- appareltype string
-- association string
-- averagerating null
-- batcertification string
-- boottype string
-- bopis string
-- breakingmap boolean
-- catalogentryid null
-- category csv
-- city string
-- clearancesku string
-- coldweather null
-- color csv # 
-- count number #
-- country string
-- disabled boolean
-- ecomshoesize string
-- ecomshoewidth string
-- exclusion string
-- features csv
-- gender string
-- genderbyage null
-- glovetype string
-- google_product_category null
-- hatstyle string
-- inseam string
-- insoleshoesize string
-- internal_brands null
-- isskurecommendation boolean, always true
-- jackettype csv
-- league csv
-- listprice_number4 number
-- longtitle1 string
-- maxlistprice number
-- maxmapprice number
-- maxofferprice number
-- maxpercentoff number
-- milbteam string
-- minmapprice number
-- minofferprice number
-- minpercentoff number
-- mlbpplayer string
-- mlbteam string
-- mlsteam null
-- nbapplayer string
-- nbateam string
-- ncaateam string
-- new_products number
-- nflpplayer string
-- nflteam string
-- nhlpplayer string
-- nhlteam string
-- nlbteam string
-- offerprice_number3 number
-- pantlength null
-- player string
-- price number
-- price_boost null
-- pricerange csv
-- product_type csv
-- producttype csv
-- promotion_exclusion boolean
-- quantity number
-- reviewcount_number2 number
-- sale string
-- shoetype string
-- singlesku null
-- size string
-- sleevelength string
-- socksize string
-- socktype null
-- sport csv
-- team string # for real football
-- teamexist string
-- teammaster csv
-- temperature string
-- title string
-- trending null
-- warmthlevel string
-- wheel_size null
-- wnbateam string
-- xflteam string
