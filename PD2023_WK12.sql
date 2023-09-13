select
    *,
    to_date(
        concat_ws(
            '-',
            right(date, 4),
            left(right(date, 7), 2),
            left(date, 2)
        )
    ) as new_date
from
    pd2023_wk12_new_customers
order by
    new_date;
select
    *
from
    pd2023_wk12_roi_new_customers
where
    reporting_date = '06/06/2022';
select
    *,
    to_date(
        concat_ws(
            '-',
            right(reporting_date, 4),
            left(right(reporting_date, 7), 2),
            left(reporting_date, 2)
        )
    ) as new_date
from
    pd2023_wk12_roi_new_customers
where
    new_date = '2022-06-06'
order by
    new_date;
create temporary table pd2023_wk12_uk_bank_holidays(year, date, bank_holiday) as
select
    case
        when year = 2022 then 2023
        when year = 2023 then 2022
        else null
    end as year,
    date,
    bank_holiday
from
    til_playground.preppin_data_inputs.pd2023_wk12_uk_bank_holidays;
select
    *
from
    til_dataschool.ds28.pd2023_wk12_uk_bank_holidays;
with t1 as(
        select
            date,
            bank_holiday,
            year_new,
            id
        from(
                -- create row id using seq() and move the year value to the succeeding row
                select
                    *,
                    seq8() as id,
                    lag(year) over(
                        order by
                            id
                    ) as year_new
                from
                    til_dataschool.ds28.pd2023_wk12_uk_bank_holidays
            )
        where
            date <> ''
    ),
    bank_holidays_tb as(
        -- forward fill the year based on its group using partition
        -- create date field for the bank holidays
        select
            date,
            bank_holiday,
            to_varchar(MIN(year_new) over(partition by grouper)) as year,
            to_varchar(month(to_date(right(date, 3), 'mon'))) as month,
            to_date(concat_ws('-', year, month, left(date, 2))) as date_new
        from(
                -- create a group for each year
                select
                    *,
                    count(year_new) over(
                        order by
                            id
                    ) as grouper
                from
                    t1
            )
        order by
            date_new
    ),
    new_customers_tb as(
        select
            to_date(
                concat_ws(
                    '-',
                    right(date, 4),
                    left(right(date, 7), 2),
                    left(date, 2)
                )
            ) as date,
            new_customers
        from
            pd2023_wk12_new_customers
    ),
    new_customers_tb_consolidated as(
        select
            bank_holiday,
            nc.*,
            DAYOFWEEKISO(nc.date) as DOW,
            case
                when DOW = 7
                or DOW = 6
                or bank_holiday is not null THEN 0
                ELSE 1
            END as "REPORTING_DAY_FLAG",
            case
                when (
                    bank_holiday is not null
                    and dow = 5
                ) then dateadd('day', 3, nc.date)
                when bank_holiday is not null then dateadd('day', 1, nc.date)
                when (
                    reporting_day_flag = 0
                    and dow = 6
                ) then dateadd('day', 2, nc.date)
                when (
                    reporting_day_flag = 0
                    and dow = 7
                ) then dateadd('day', 1, nc.date)
                else nc.date
            end as reporting_date_temp
        from
            bank_holidays_tb as bh
            right join new_customers_tb as nc on nc.date = bh.date_new
    ),
    new_customers_reporting_consolidated1 as(
        select
            new_customers,
            case
                when (
                    date_new is not null
                    and DAYOFWEEKISO(reporting_date_temp) = 5
                ) then dateadd('day', 3, reporting_date_temp)
                when date_new is not null then dateadd('day', 1, reporting_date_temp)
                else reporting_date_temp
            end as reporting_date_temp2
        from
            new_customers_tb_consolidated
            left join bank_holidays_tb on reporting_date_temp = date_new
    )
    --select * from new_customers_reporting_consolidated1;
,
    new_customers_reporting_consolidated2 as(
        select
            new_customers,
            case
                when (
                    date_new is not null
                    and DAYOFWEEKISO(reporting_date_temp2) = 5
                ) then dateadd('day', 3, reporting_date_temp2)
                when date_new is not null then dateadd('day', 1, reporting_date_temp2)
                else reporting_date_temp2
            end as reporting_date_temp3
        from
            new_customers_reporting_consolidated1 as ncc
            left join bank_holidays_tb as bh on reporting_date_temp2 = date_new
    ),
    new_customers_reporting_consolidated3 as(
        select
            new_customers,
            case
                when (
                    bh.date_new is not null
                    and DAYOFWEEKISO(reporting_date_temp3) = 5
                ) then dateadd('day', 3, reporting_date_temp3)
                when bh.date_new is not null then dateadd('day', 1, reporting_date_temp3)
                else reporting_date_temp3
            end as reporting_date
        from
            new_customers_reporting_consolidated2 as ncc
            left join bank_holidays_tb as bh on reporting_date_temp3 = bh.date_new
    ),
    new_customers_sum as(
        -- calculate the sum of new customer by reporting date
        select
            dateadd('day', 1, reporting_date) as next_reporting_date,case
                when DAYOFWEEKISO(next_reporting_date) = 6 then concat_ws(
                    '-',
                    to_char(dateadd('day', 3, reporting_date), 'MMMM'),
                    year(dateadd('day', 3, reporting_date))
                )
                else concat_ws(
                    '-',
                    to_char(dateadd('day', 1, reporting_date), 'MMMM'),
                    year(dateadd('day', 1, reporting_date))
                )
            end as reporting_month,
            reporting_date,
            sum(new_customers) as new_customers
        from
            new_customers_reporting_consolidated3
        group by
            reporting_date
    ),
    combined_tb as(
        select
            * exclude next_reporting_date
        from
            new_customers_sum as nc
            left join (
                select
                    new_customers as roi_new_customers,
                    reporting_month as roi_reporting_month,
                    to_date(
                        concat_ws(
                            '-',
                            right(reporting_date, 4),
                            left(right(reporting_date, 7), 2),
                            left(reporting_date, 2)
                        )
                    ) as new_date
                from
                    pd2023_wk12_roi_new_customers
            ) as roi_tb on nc.reporting_date = new_date
    )
select
    case
        when concat_ws(
            '-',
            left(reporting_month, 3),
            right(reporting_month, 2)
        ) = roi_reporting_month then ''
        else 'X'
    end as misalignment_flag,
    reporting_month,
    row_number() over(
        partition by reporting_month
        order by
            reporting_date
    ) as reporting_day,
    reporting_date,
    new_customers,
    COALESCE(roi_new_customers, 0) as roi_new_customers,
    roi_reporting_month
from
    combined_tb
where
    to_number(right(reporting_month, 4)) < 2024;