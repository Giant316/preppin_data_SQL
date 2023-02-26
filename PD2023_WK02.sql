-- join two tables on Bank 
-- remove sort code field which contains dashes using replace function
-- assign GB as starting character since all transactions take place in the UK
-- cast the account number to VARCHAR data type to be concatenated with other strings
select ft.TRANSACTION_ID as "Transaction ID", concat('GB',SWIFT_CODE, replace(SORT_CODE, '-', ''), ACCOUNT_NUMBER::VARCHAR) as IBAN  from PD2023_WK02_SWIFT_CODES as sc right join PD2023_WK02_TRANSACTIONS as ft on sc.bank = ft.bank