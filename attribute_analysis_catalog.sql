WITH 
non_archived_retailer AS (
    SELECT DISTINCT retailer_id, retailer_name
    FROM config_account
    WHERE archived = 0
)
SELECT DISTINCT 
-- c.title, c.brand, c.id, c.item_group_id,
-- c.custom:Addon_Cat_4
-- c.* 
-- c.custom:mpn
-- c.custom:Feature
-- energy_efficiency_class
-- c.custom:
-- product_type
-- count(distinct c.item_group_id),
-- count(distinct c.id)
-- c.item_group_id,
-- c.custom:vehicle
-- c.custom:storeid
-- count(distinct c.custom:vehicle)
-- c.custom:instock_store
-- c.custom:ends_in_99
-- c.dataset_id,
-- c.custom:categories
-- c.custom:gl_lens_type
-- c.custom:colour
-- c.custom:car_ids
-- c.*,
-- c.item_group_id,
-- c.id,
-- c.custom:rms_subclass,
-- c.gender
c.*
FROM product_catalog c
JOIN config_dataset_data_expiration AS ex ON ex.dataset_id = c.dataset_id
JOIN config_dataset_info AS i ON i.dataset_id = c.dataset_id 
JOIN non_archived_retailer AS nar ON c.retailer_id = nar.retailer_id
WHERE c.update_time >= ex.cutoff_time
  AND i.archive_time IS NULL 
  -- AND c.retailer_id = 1368
  -- AND c.retailer_id = 1393
  -- AND c.retailer_id=1368
  -- AND c.retailer_id=137
  -- AND c.retailer_id=1382
  -- and c.retailer_id=1211
  -- and c.dataset_id=78489
  -- AND c.custom:instock_store LIKE '%,%'
  -- and c.id in ('741613', '657999', '690356', '627603', '0')
  -- and c.retailer_id = 1416  -- sportsman's guide
  -- and c.retailer_id = 1316  -- academy
  -- and c.item_group_id = '201727787'
-- and c.retailer_id=1393
-- and c.custom:longtitle1 like '%, and%'
and c.retailer_id=1388
and c.dataset_id=65007
and c.custom:l1_category = 'Beer, Wine & Liquor'
  -- and c.custom:rms_subclass = 'Thermals'
  -- and c.gender = 'Men'
  -- and c.custom:rms_class is not null
  -- and c.custom:categories like '%\"%'
  -- and c.retailer_id=249
  -- and c.dataset_id=72327
  -- and c.dataset_id=68831
  -- and c.retailer_id=1475
  -- and c.retailer_id=1205
  -- and c.custom:cut_capacity is not null
  -- and c.custom:Phase LIKE '%,%'
  -- and c.retailer_id=1397
  -- AND c.custom:optional_charge LIKE '%,%'
  -- 99107782739
  -- AND c.retailer_id = 249
  -- AND c.dataset_id = 72327
  -- AND c.id=99107782739
  -- AND c.custom:collections = 'Andros'
  -- AND c.custom:by_construction 
  -- AND c.custom:vehicle = c.custom:vehiclealt
  -- AND c.custom:vehicle LIKE '%,%'
  -- AND c.custom:storeid LIKE '%,%'
  -- AND c.dataset_id=85385
  -- AND c.item_group_id='L91984'
  -- and c.retailer_id=1416
  -- and c.dataset_id=12857
  -- AND c.dataset_id=56793
  -- and c.custom:Fitment != ''
  -- -- and c.id = '1500000037261'
  -- and c.custom:mpn like '%,%'
  -- and mpn like '%,%'
  -- and c.custom:Addon_Cat_4 like '%,%'
-- order by c.item_group_id, c.id
limit 1000
;

WITH 
non_archived_retailer AS (
    SELECT DISTINCT retailer_id, retailer_name
    FROM config_account
    WHERE archived = 0
)
SELECT DISTINCT
    array_size(split(c.custom:optional_charge, ',')), count(*)
FROM product_catalog c
JOIN config_dataset_data_expiration AS ex ON ex.dataset_id = c.dataset_id
JOIN config_dataset_info AS i ON i.dataset_id = c.dataset_id 
JOIN non_archived_retailer AS nar ON c.retailer_id = nar.retailer_id
WHERE c.update_time >= ex.cutoff_time
  AND i.archive_time IS NULL 
  and c.retailer_id=1397
  AND c.custom:optional_charge LIKE '%,%'
group by 1
order by 1 asc
;

WITH 
non_archived_retailer AS (
    SELECT DISTINCT retailer_id, retailer_name
    FROM config_account
    WHERE archived = 0
)
SELECT DISTINCT
    avg(array_size(split(c.custom:optional_charge, ',')))
FROM product_catalog c
JOIN config_dataset_data_expiration AS ex ON ex.dataset_id = c.dataset_id
JOIN config_dataset_info AS i ON i.dataset_id = c.dataset_id 
JOIN non_archived_retailer AS nar ON c.retailer_id = nar.retailer_id
WHERE c.update_time >= ex.cutoff_time
  AND i.archive_time IS NULL 
  and c.retailer_id=1397
  AND c.custom:optional_charge LIKE '%,%'
;