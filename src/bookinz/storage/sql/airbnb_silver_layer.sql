-- airbnb_silver_layer.sql
-- Transforms the AirBnB bronze layer into the silver (cleaned, deduplicated) layer.
--
-- Key differences from the Booking.com silver transform:
--   • Source view  : airbnb_bronze   (not bronze)
--   • rating       : multiplied by 2.0  →  0–10 scale (matching Booking.com silver)
--   • num_rooms_available : always NULL  (AirBnB does not expose this field reliably)
--   • distance_from_center_km : NULL in SQL; Python fills it via geopy geocoding
--   • latitude / longitude  : passed through as temp columns so Python can geocode
--   • _pk prefix   : 'ABNB'          (Booking.com uses 'BOOK')
--   • _source      : 'ABNB'
--
-- The final silver column set is identical to the Booking.com silver schema so
-- that both sources can be unioned in the gold layer.

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
        -- Pass lat/lon through for Python geocoding; not written to silver Parquet
        latitude,
        longitude,
        checkin_date,
        checkout_date,
        date_diff('day', cast(checkin_date as date), cast(checkout_date as date)) as num_nights,
        num_adults,
        case
            when currency = 'USD' then 'USD'
            when currency = 'EUR' then 'EUR'
            when currency = 'GBP' then 'GBP'
            when currency = '$'   then 'USD'
            when currency = '€'   then 'EUR'
            when currency = '£'   then 'GBP'
            else currency
        end as currency,
        total_price,
        -- Normalise AirBnB 0–5 rating to 0–10 to match Booking.com silver
        rating * 2.0                       as rating,
        num_reviews,
        -- AirBnB does not expose remaining room counts
        cast(null as bigint)               as num_rooms_available,
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
    from airbnb_accommodation_bronze
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
        latitude,
        longitude,
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
        -- Placeholder: Python replaces with geopy-computed value after this query runs
        cast(null as double) as distance_from_center_km,
        -- Temp columns consumed by Python; dropped before writing Parquet
        latitude,
        longitude,
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
        'ABNB' || '/' ||
            cast(replace(lower(city),    ' ', '_') as varchar) ||
            cast(replace(upper(country), ' ', '_') as varchar) || '/' ||
            cast(replace(lower(facility_id), '-', '_') as varchar) || '/' ||
            cast(replace(checkin_date, '-', '') as varchar) || '_' ||
            cast(replace(checkout_date, '-', '') as varchar) || '/' ||
            replace(cast(scrape_date as varchar), '-', '') as _pk,
        'ABNB' as _source
    from dedup_data
)
select *
from silver;
