create table if not exists datawarehouse.transactions
(
    account_number      varchar(255),
    transaction_date    date,
    transaction_details varchar(1000),
    withdrawal_amt      double precision,
    deposit_amt         double precision,
    balance_amt         double precision,
    load_timestamp      timestamp default current_timestamp
);