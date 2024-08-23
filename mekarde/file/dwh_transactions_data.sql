select REGEXP_REPLACE(account_no, '[^0-9]', '')     as account_number
     , TO_DATE("date", 'YYYY-MM-DD')                as transaction_date
     , transaction_details                          as transaction_details
     , nullif(withdrawal_amt, '')::double precision as withdrawal_amt
     , nullif(deposit_amt, '')::double precision    as deposit_amt
     , nullif(balance_amt, '')::double precision    as balance_amt
from staging.transactions
where load_timestamp >= current_date - 1