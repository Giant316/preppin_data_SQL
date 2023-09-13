select
    branch,
    branch_long,
    branch_lat,
    distance,
    rank() over(
        partition by branch
        order by
            distance
    ) as "CUSTOMER PRIORITY",
    customer,
    address_long,
    address_lat
from(
        // Transform the latitude and longitude into radians
        select
            *,
            branch_long /(180 / pi()) as BRANCH_LONG_RAD,
            branch_lat /(180 / pi()) as BRANCH_LAT_RAD,
            ADDRESS_LONG /(180 / pi()) as ADDRESS_LONG_RAD,
            ADDRESS_LAT /(180 / pi()) as ADDRESS_LAT_RAD,
            ROUND(
                3963 * acos(
                    (sin(BRANCH_LAT_RAD) * sin(ADDRESS_LAT_RAD)) + cos(BRANCH_LAT_RAD) * cos(ADDRESS_LAT_RAD) * cos(ADDRESS_LONG_RAD - BRANCH_LONG_RAD)
                ),
                2
            ) as distance,
            //Find the closest Branch for each Customer
            rank() over(
                partition by customer
                order by
                    distance
            ) as closest
        from
            pd2023_wk11_dsb_customer_locations
            cross join pd2023_wk11_dsb_branches
        order by
            customer,
            distance asc
    )
where
    closest = 1
order by
    branch,
    distance asc;