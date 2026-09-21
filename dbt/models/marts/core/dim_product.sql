-- Grain: product + sub_product. product_group collapses names the CFPB has
-- renamed over the years (e.g. 2017 and 2023 taxonomy changes) so trends line up.
with products as (

    select distinct
        product,
        sub_product
    from {{ ref('int_complaints__enriched') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['product', 'sub_product']) }} as product_key,
    product,
    sub_product,
    case
        when product ilike 'Credit reporting%' then 'Credit reporting'
        when product ilike 'Credit card%' then 'Credit card'
        when product = 'Prepaid card' then 'Prepaid card'
        when product in (
                'Checking or savings account',
                'Bank account or service'
            ) then 'Checking or savings'
        when product ilike 'Money transfer%'
            or product = 'Virtual currency' then 'Money transfer & crypto'
        when product ilike 'Payday loan%'
            or product = 'Consumer Loan' then 'Payday & personal loans'
        when product = 'Vehicle loan or lease' then 'Vehicle loan'
        when product is null then 'Unknown'
        else product
    end                                                                as product_group
from products
