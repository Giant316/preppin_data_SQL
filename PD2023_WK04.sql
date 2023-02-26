-- create a SQL statement that produce a temporary view from all the unioned tables in Week 4
with month_list(month_id, month_num) as
(SELECT upper(to_char(dateadd(month,-seq8(0), DATE_TRUNC('month', CURRENT_DATE))::timestamp, 'MMMM')) as month, month(dateadd(month,-seq8(0), DATE_TRUNC('month', CURRENT_DATE))::timestamp) AS month_num
FROM table (generator(rowcount => 12)) order by month_num),

sql_statement(month_id, statement) as
(select 0, concat('create or replace temporary view ', split_part('PD2023_WK04_','.',-1), 1,'_', 12, ' as ')
union all
select month_num, concat('select * exclude JOINING_DAY, to_date(concat_ws(\'-\',\'2023\',', month_num::string, ',JOINING_DAY::STRING)) as JOINING_DAY from ', 'PD2023_WK04_',month_id, 
                case when month_num=12::int then ';' else ' union all ' end)
from month_list)

select listagg(statement, '\n') within group (order by month_id) from sql_statement


-----------------------------------------------------------------------------
-- create a temporary view that union all 12 tables from week 4 
create or replace temporary view PD2023_WK04_1_12 as 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',1,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_JANUARY union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',2,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_FEBRUARY union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',3,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_MARCH union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',4,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_APRIL union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',5,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_MAY union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',6,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_JUNE union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',7,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_JULY union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',8,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_AUGUST union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',9,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_SEPTEMBER union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',10,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_OCTOBER union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',11,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_NOVEMBER union all 
select * exclude JOINING_DAY, to_date(concat_ws('-','2023',12,JOINING_DAY::STRING)) as JOINING_DAY from PD2023_WK04_DECEMBER;

-------------------------------------------------------------------------------------------------------------------------------

with WK04_NewCustomer(ID, "Joining Date", "Ethnicity", "Account Type", "Date of Birth") as (
select * from PD2023_WK04_1_12 pivot(min(VALUE) for DEMOGRAPHIC in ('Ethnicity', 'Account Type', 'Date of Birth'))
)
select * exclude "Date of Birth", to_date(concat_ws('-', split_part("Date of Birth", '/', 3), split_part("Date of Birth", '/', 1), split_part("Date of Birth", '/', 2))) as "Date of Birth" from WK04_NewCustomer order by ID