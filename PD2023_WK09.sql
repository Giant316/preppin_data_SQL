-- filter out the cancelled transactions
with WK09_TRANSACTION as(
select tp.* exclude TRANSACTION_ID, to_date(td.TRANSACTION_DATE) as TRANSACTION_DATE, td.VALUE from PD2023_WK07_TRANSACTION_DETAIL as td inner join PD2023_WK07_TRANSACTION_PATH as tp on td.TRANSACTION_ID = tp.TRANSACTION_ID where CANCELLED_ <> 'Y'
),
--retrieve the incoming transactions for each account
WK09_INCOMING as(
select ai.* exclude (ACCOUNT_TYPE, ACCOUNT_HOLDER_ID), t.TRANSACTION_DATE, t.VALUE as INCOMING from PD2023_WK07_ACCOUNT_INFORMATION as ai inner join WK09_TRANSACTION as t on ai.ACCOUNT_NUMBER = t.ACCOUNT_TO order by ai.ACCOUNT_NUMBER, t.TRANSACTION_DATE, INCOMING
),
-- retrieve the outgoing transaction for each account
WK09_OUTGOING as(
select ai.* exclude (ACCOUNT_TYPE, ACCOUNT_HOLDER_ID), t.TRANSACTION_DATE, t.VALUE*-1 as OUTGOING from PD2023_WK07_ACCOUNT_INFORMATION as ai inner join WK09_TRANSACTION as t on ai.ACCOUNT_NUMBER = t.ACCOUNT_FROM order by ai.ACCOUNT_NUMBER, t.TRANSACTION_DATE, OUTGOING
),
-- combine the incoming and outgoing transactions in tall structure in order to perform a running sum subsequently
WK09_ALL as(
select ACCOUNT_NUMBER, TRANSACTION_DATE as BALANCE_DATE, INCOMING as BALANCE from WK09_INCOMING UNION ALL 
select ACCOUNT_NUMBER, BALANCE_DATE, BALANCE from WK09_INCOMING qualify row_number() over (partition by ACCOUNT_NUMBER, BALANCE_DATE, BALANCE order by ACCOUNT_NUMBER, BALANCE_DATE, BALANCE) = 1 UNION ALL
select ACCOUNT_NUMBER, TRANSACTION_DATE as BALANCE_DATE, OUTGOING as BALANCE from WK09_OUTGOING UNION ALL
select ACCOUNT_NUMBER, BALANCE_DATE, BALANCE from WK09_OUTGOING qualify row_number() over (partition by ACCOUNT_NUMBER, BALANCE_DATE, BALANCE order by ACCOUNT_NUMBER, BALANCE_DATE, BALANCE) = 1
),
-- remove the duplicates (the starting balance on 2023-01-31, there will be duplicates when the accounts have both incoming and outgoing transactions)
WK09_NO_DUP as(
select DISTINCT* from WK09_ALL order by ACCOUNT_NUMBER, BALANCE_DATE, BALANCE desc
),
WK09 as(
select ACCOUNT_NUMBER as "Account Number", BALANCE_DATE as "Balance Date", IFF(BALANCE_DATE = '2023-01-31', NULL, BALANCE) as "Transaction Value", SUM(BALANCE) OVER(partition by ACCOUNT_NUMBER order by BALANCE_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "Balance" from WK09_NO_DUP order by ACCOUNT_NUMBER, BALANCE_DATE, BALANCE desc
),
-- Get the account balance where no transaction occurs (Left Outer Join without the intersection)
ACC_NO_TRANSACTION as(
select ACCOUNT_NUMBER, BALANCE_DATE, BALANCE,  NULL as "Transaction Value" from PD2023_WK07_ACCOUNT_INFORMATION LEFT JOIN WK09 on ACCOUNT_NUMBER = "Account Number" WHERE WK09."Account Number" IS NULL
),
-- combine all accounts (with/without transactions) as final output
WK09_OUTPUT as(
select * from WK09 UNION ALL select * from ACC_NO_TRANSACTION
)
select * from WK09_OUTPUT order by "Account Number", "Balance Date", "Transaction Value"