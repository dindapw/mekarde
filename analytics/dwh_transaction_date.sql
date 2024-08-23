select transaction_date,
       min(balance_amt)      as min_balance_amt,
       max(balance_amt)      as max_balance_amt,
       count(1)              as no_of_transactions,
       count(account_number) as no_of_accounts
from datawarehouse.transactions
group by transaction_date
order by transaction_date;