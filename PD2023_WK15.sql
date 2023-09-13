with t1 (id, "22","23","24","25","26","27","28","29","30","31") as(
select seq8() as id, case when replace(X_3, ' ', '') = '' then null else replace(X_3, ' ', '') end::integer as X_3, X_4, X_5, X_6, X_7, X_8, X_9, X_10, X_11, X_12 from pd2023_wk15_easter_dates where id > 2 and id < 29
),
march_tb as(
select * from(
select * exclude id from t1)unpivot (year for day in ("22","23","24","25","26","27","28","29","30","31"))
),
t2 (id, "01", "01_2", "02","03","04","05", "06","07","08","09", "10","11", "12","13","14","15", "16","17","18","19","20","21", "22","23","24","25") as(
select seq8() as id, case when replace(X_13, ' ', '') = '' then null else replace(X_13, ' ', '') end::integer as X_13_2, * exclude (X, X_1, X_2, X_3, X_4, X_5, X_6, X_7, X_8, X_9, X_10, X_11, X_12) from pd2023_wk15_easter_dates where ID > 2 and ID < 30
),
april_tb as(
select * from(
select * exclude (id, "01_2") from t2)unpivot (year for day in ("01", "02","03","04","05", "06","07","08","09", "10","11", "12","13","14","15", "16","17","18","19","20","21", "22","23","24","25")))
select row_number()over(order by easter_sunday) as id, * from(
select to_date(concat_ws('-', year::varchar, '04', day)) as easter_sunday from april_tb
union all
select to_date(concat_ws('-', year::varchar, '03', day)) as easter_sunday from march_tb
) where easter_sunday < '2023-05-01';