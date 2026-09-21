-- Grain: state + 3-digit ZIP prefix (coarse enough to keep the dimension small).
with geos as (

    select distinct
        state,
        zip3
    from {{ ref('int_complaints__enriched') }}

),

states as (

    select * from {{ ref('state_reference') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['g.state', 'g.zip3']) }} as geography_key,
    g.state,
    g.zip3,
    s.state_name,
    coalesce(s.census_region, 'Other / unknown')                  as census_region,
    s.population_2020                                             as state_population_2020,
    s.state is not null                                           as is_known_state
from geos as g
left join states as s
    on g.state = s.state
