-- silver_layer.sql
-- Transforms the bronze layer into the silver (cleaned, deduplicated) layer.
--
-- Logic:
--   1. data_enriched  — type-casts + derived columns, adds ROW_NUMBER for dedup.
--   2. dedup_data     — keeps only the freshest scrape per (city, country,
--                       facility_id, checkin_date, checkout_date) tuple.
--   3. silver         — adds per-adult / per-night price metrics, normalises
--                       the quality-price score to [0, 1], and builds a
--                       surrogate primary key (_pk).

with
data_enriched as (
    select
        facility_id,
        url,
        name,
        accommodation_type,
        split_part(search_area, '__', 1) as city,
        split_part(search_area, '__', 2) as country,
        neighbourhood,
        distance_from_center_km,
        checkin_date,
        checkout_date,
        date_diff('day', cast(checkin_date as date), cast(checkout_date as date)) as num_nights,
        num_adults,
        case
            when currency = '€' then 'EUR'
            when currency = '$' then 'USD'
            else currency
        end as currency,
        total_price,
        rating,
        num_reviews,
        num_rooms_available,
        is_available,
        scrape_date,
        ROW_NUMBER() OVER (
            PARTITION BY
                split_part(search_area, '__', 1),
                split_part(search_area, '__', 2),
                facility_id,
                checkin_date,
                checkout_date
            ORDER BY scrape_date DESC
        ) AS rn
    from booking_accommodation_bronze
),
dedup_data as (
    select
        facility_id,
        url,
        name,
        accommodation_type,
        city,
        country,
        neighbourhood,
        distance_from_center_km,
        checkin_date,
        checkout_date,
        num_nights,
        num_adults,
        currency,
        total_price,
        round(total_price / num_nights, 2)                       as price_per_night,
        round(total_price / num_adults, 2)                       as price_per_adult,
        round(total_price / (num_adults * num_nights), 2)        as price_per_adult_per_night,
        rating,
        rating / round(total_price / (num_adults * num_nights), 2) as quality_price_score,
        num_reviews,
        num_rooms_available,
        is_available,
        scrape_date
    from data_enriched
    where rn = 1
),
silver as (
    select
        facility_id,
        url,
        name,
        accommodation_type,
        city,
        country,
        neighbourhood,
        distance_from_center_km,
        checkin_date,
        checkout_date,
        num_nights,
        num_adults,
        currency,
        total_price,
        price_per_night,
        price_per_adult,
        price_per_adult_per_night,
        rating,
        (
            (quality_price_score - MIN(quality_price_score) OVER ())
        )
        /
        (
            NULLIF(MAX(quality_price_score) OVER () - MIN(quality_price_score) OVER (), 0)
        ) AS quality_price_score,
        num_reviews,
        num_rooms_available,
        is_available,
        'BOOK' || '/' ||
            cast(replace(lower(city), ' ', '_') as varchar) ||
            cast(replace(upper(country), ' ', '_') as varchar) || '/' ||
            cast(replace(lower(facility_id), '-', '_') as varchar) || '/' ||
            cast(replace(checkin_date, '-', '') as varchar) || '_' ||
            cast(replace(checkout_date, '-', '') as varchar) || '/' ||
            replace(cast(scrape_date as varchar), '-', '') as _pk,
        'BOOK' as _source
    from dedup_data
)
select *
from silver;
