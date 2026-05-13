-- airbnb_silver_layer.sql
-- Transforms the AirBnB bronze layer into the silver (cleaned, deduplicated) layer.
--
-- Key differences from the Booking.com silver transform:
--   • Source views : airbnb_accommodation_bronze  (search results)
--                    airbnb_facility_bronze        (detail pages — precise lat/lon)
--                    city_centres                  (Python-injected table with geocoded
--                                                   city-centre coordinates per city/country)
--   • rating       : multiplied by 2.0  →  0–10 scale (matching Booking.com silver)
--   • num_rooms_available : always NULL  (AirBnB does not expose this field reliably)
--   • distance_from_center_km : computed in SQL via haversine using facility lat/lon
--                                (falls back to accommodation bronze lat/lon when the
--                                 facility page has not been scraped yet)
--   • _pk prefix   : 'ABNB'          (Booking.com uses 'BOOK')
--   • _source      : 'ABNB'
--
-- city_centres must be registered in the DuckDB connection before this query runs:
--   CREATE TABLE city_centres (city VARCHAR, country VARCHAR,
--                               centre_lat DOUBLE, centre_lon DOUBLE);
--
-- Haversine formula (returns km):
--   R = 6371
--   Δlat = radians(lat2 - lat1)
--   Δlon = radians(lon2 - lon1)
--   a = sin²(Δlat/2) + cos(lat1_r)·cos(lat2_r)·sin²(Δlon/2)
--   d = 2·R·asin(sqrt(a))

with
data_enriched as (
    select
        a.facility_id,
        a.url,
        a.name,
        a.accommodation_type,
        split_part(a.search_area, '__', 1) as city,
        split_part(a.search_area, '__', 2) as country,
        a.neighbourhood,
        -- Precise coordinates: prefer facility detail page; fall back to search result
        coalesce(f.latitude,  a.latitude)  as listing_lat,
        coalesce(f.longitude, a.longitude) as listing_lon,
        a.checkin_date,
        a.checkout_date,
        date_diff('day', cast(a.checkin_date as date), cast(a.checkout_date as date)) as num_nights,
        a.num_adults,
        case
            when a.currency = 'USD' then 'USD'
            when a.currency = 'EUR' then 'EUR'
            when a.currency = 'GBP' then 'GBP'
            when a.currency = '$'   then 'USD'
            when a.currency = '€'   then 'EUR'
            when a.currency = '£'   then 'GBP'
            else a.currency
        end as currency,
        a.total_price,
        -- Normalise AirBnB 0–5 rating to 0–10 to match Booking.com silver
        a.rating * 2.0                     as rating,
        a.num_reviews,
        -- AirBnB does not expose remaining room counts
        cast(null as bigint)               as num_rooms_available,
        a.is_available,
        a.scrape_date,
        ROW_NUMBER() OVER (
            PARTITION BY
                split_part(a.search_area, '__', 1),
                split_part(a.search_area, '__', 2),
                a.facility_id,
                a.checkin_date,
                a.checkout_date
            ORDER BY a.scrape_date DESC
        ) AS rn
    from airbnb_accommodation_bronze a
    left join (
        -- Most recent facility record per listing (lat/lon doesn't change)
        select facility_id, latitude, longitude
        from (
            select facility_id, latitude, longitude,
                   ROW_NUMBER() OVER (PARTITION BY facility_id ORDER BY scraped_at DESC) as rn_f
            from airbnb_facility_bronze
            where latitude is not null and longitude is not null
        ) ranked
        where rn_f = 1
    ) f on a.facility_id = f.facility_id
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
        listing_lat,
        listing_lon,
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
        d.facility_id,
        d.url,
        d.name,
        d.accommodation_type,
        d.city,
        d.country,
        d.neighbourhood,
        -- Haversine distance from city centre (km); NULL when coords unavailable
        case
            when d.listing_lat is null or d.listing_lon is null
              or c.centre_lat  is null or c.centre_lon  is null
            then cast(null as double)
            else round(
                2.0 * 6371.0 * asin(sqrt(
                    power(sin(radians(d.listing_lat - c.centre_lat) / 2.0), 2)
                    + cos(radians(c.centre_lat))
                    * cos(radians(d.listing_lat))
                    * power(sin(radians(d.listing_lon - c.centre_lon) / 2.0), 2)
                )),
                3
            )
        end as distance_from_center_km,
        d.checkin_date,
        d.checkout_date,
        d.num_nights,
        d.num_adults,
        d.currency,
        d.total_price,
        d.price_per_night,
        d.price_per_adult,
        d.price_per_adult_per_night,
        d.rating,
        (
            (d.quality_price_score - MIN(d.quality_price_score) OVER ())
        )
        /
        (
            NULLIF(MAX(d.quality_price_score) OVER () - MIN(d.quality_price_score) OVER (), 0)
        ) AS quality_price_score,
        d.num_reviews,
        d.num_rooms_available,
        d.is_available,
        'ABNB' || '/' ||
            cast(replace(lower(d.city),    ' ', '_') as varchar) ||
            cast(replace(upper(d.country), ' ', '_') as varchar) || '/' ||
            cast(replace(lower(d.facility_id), '-', '_') as varchar) || '/' ||
            cast(replace(d.checkin_date, '-', '') as varchar) || '_' ||
            cast(replace(d.checkout_date, '-', '') as varchar) || '/' ||
            replace(cast(d.scrape_date as varchar), '-', '') as _pk,
        'ABNB' as _source
    from dedup_data d
    left join city_centres c on d.city = c.city and d.country = c.country
)
select *
from silver;
