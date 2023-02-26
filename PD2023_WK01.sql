-- split the transaction code using delimiter and take only the first part
-- assign string of online or in-person based on its numeric value
-- use decode function instead of dayname function to get the full name of the day of week
with WK01 as(
SELECT split_part(TRANSACTION_CODE, '-', 1) as "Bank", CUSTOMER_CODE, iff(ONLINE_OR_IN_PERSON = 1, 'Online', 'In-Person') as "Online or In-Person", DECODE(EXTRACT ('dayofweek_iso',concat_ws('-', right(left(TRANSACTION_DATE,10), 4), right(left(TRANSACTION_DATE,5), 2), left(left(TRANSACTION_DATE,10), 2))::TIMESTAMP_NTZ),
  1, 'Monday',
  2, 'Tuesday',
  3, 'Wednesday',
  4, 'Thursday',
  5, 'Friday',
  6, 'Saturday',
  7, 'Sunday') as "Transaction Date", value from PD2023_WK01
)

-- Output 1 Total Values of Transactions by group by each bank
select "Bank", sum(value) as "Value" from WK01 group by "Bank";

-- Output 2: Total Values group by Bank, Day of the Week and Type of Transaction
select "Bank", "Online or In-Person", "Transaction Date", sum(value) as "Value" from WK01 group by "Bank", "Online or In-Person", "Transaction Date" order by "Value";

-- Output 3: Total Values group by Bank and Customer Code
select "Bank", CUSTOMER_CODE as "Customer Code", sum(value) as "Value" from WK01 group by "Bank", "Customer Code";