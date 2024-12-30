
-- Создание стейджинговых таблиц
CREATE TABLE lubg_stg_clients
(
    client_id    VARCHAR,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    PRIMARY KEY (client_id)
);

CREATE TABLE lubg_stg_accounts
(
    account_num  VARCHAR,
    valid_to TIMESTAMP,
    client VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    PRIMARY KEY (account_num),
    CONSTRAINT "client_fk" FOREIGN KEY (client) REFERENCES lubg_stg_clients(client_id)
);

CREATE TABLE lubg_stg_cards
(
    card_num    VARCHAR,
    account_num VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    PRIMARY KEY (card_num),
    CONSTRAINT "account_fk" FOREIGN KEY (account_num) REFERENCES lubg_stg_accounts(account_num)
);

CREATE TABLE lubg_stg_terminals
(
    terminal_id    VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    PRIMARY KEY (terminal_id)
);


CREATE TABLE lubg_stg_transactions
(
    trans_id    VARCHAR,
    trans_date  TIMESTAMP,
    card_num    VARCHAR,
    oper_type   VARCHAR,
    amt         NUMERIC,
    oper_result VARCHAR,
    terminal    VARCHAR,
    PRIMARY KEY (trans_id),
    CONSTRAINT "card_fk" FOREIGN KEY (card_num) REFERENCES lubg_stg_cards(card_num),
    CONSTRAINT "terminal_fk" FOREIGN KEY (terminal) REFERENCES lubg_stg_terminals(terminal_id)
);


CREATE TABLE lubg_stg_blacklist
(
    passport_num  VARCHAR,
    entry_dt TIMESTAMP
);

# Создание таблиц фактов и измерений в ДВХ

CREATE TABLE lubg_dwh_dim_clients
(
    client_id    VARCHAR,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    PRIMARY KEY (client_id)
);

CREATE TABLE lubg_dwh_dim_accounts
(
    account_num    VARCHAR,
    valid_to DATE,
    client VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    PRIMARY KEY (account_num),
    CONSTRAINT "client_fk" FOREIGN KEY (client) REFERENCES lubg_dwh_dim_clients(client_id)
);

CREATE TABLE lubg_dwh_dim_cards
(
    card_num    VARCHAR,
    account_num VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    PRIMARY KEY (card_num),
    CONSTRAINT "account_fk" FOREIGN KEY (account_num) REFERENCES lubg_dwh_dim_accounts(account_num)
);

CREATE TABLE lubg_dwh_dim_terminals
(
    terminal_id    VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    create_dt TIMESTAMP,
    update_dt TIMESTAMP,
    PRIMARY KEY (terminal_id)
);

CREATE TABLE lubg_dwh_fact_transactions
(
    trans_id    VARCHAR,
    trans_date  TIMESTAMP,
    card_num    VARCHAR,
    oper_type   VARCHAR,
    amt         NUMERIC,
    oper_result VARCHAR,
    terminal    VARCHAR,
    PRIMARY KEY (trans_id),
    CONSTRAINT "card_fk" FOREIGN KEY (card_num) REFERENCES lubg_dwh_dim_cards(card_num),
    CONSTRAINT "terminal_fk" FOREIGN KEY (terminal) REFERENCES lubg_dwh_dim_terminals(terminal_id)
);

CREATE TABLE lubg_dwh_fact_blacklist
(
    passport_num  VARCHAR,
    entry_dt TIMESTAMP
);


# Создание таблиц hist SCD2

CREATE TABLE lubg_dwh_dim_clients_hist
(
    client_id    VARCHAR,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    deleted BOOL,
    PRIMARY KEY(client_id)
);

CREATE TABLE lubg_dwh_dim_accounts_hist
(
    account_num    VARCHAR,
    valid_to DATE,
    client VARCHAR,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    deleted BOOL,
    PRIMARY KEY(account_num)
);

CREATE TABLE lubg_dwh_dim_cards_hist
(
    card_num    VARCHAR,
    account_num VARCHAR,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    deleted BOOL,
    PRIMARY KEY (card_num)
);

CREATE TABLE lubg_dwh_dim_terminals_hist
(
    terminal_id    VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    deleted BOOL,
    PRIMARY KEY (terminal_id)
);
# Создание таблицы отчета

create table lubg_rep_fraud
(
	event_dt TIMESTAMP,
	passport VARCHAR,
	fio VARCHAR,
	phone VARCHAR,
	event_type VARCHAR,
	report_dt DATE
)

#Создание таблицы мета-данных
    create table lubg_meta_dwh (
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp
);

insert into
lubg_meta_dwh( schema_name, table_name, max_update_dt)
values ('public','lubg_dwh_fact_transactions', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_fact_blacklist', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_dim_terminals', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_dim_clients', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_dim_accounts', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_dim_cards', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_dim_terminals_hist', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_dim_clients_hist', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_dim_accounts_hist', to_timestamp('1900-01-01','YYYY-MM-DD')),
('public','lubg_dwh_dim_cards_hist', to_timestamp('1900-01-01','YYYY-MM-DD')),


#Дата получения данных
create table lubg_last_report_date
    (dt VARCHAR(10));

insert into lubg_last_report_date
values ('01032021')







