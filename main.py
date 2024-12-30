import psycopg2
import pandas as pd
import os
from datetime import datetime
from datetime import timedelta

conn = psycopg2.connect(database="db",
                        host="rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
                        user="hseguest",
                        password="hsepassword",
                        port="6432")

conn.autocommit = False
cursor = conn.cursor()

cursor.execute('select dt from lubg_last_report_date')
data = cursor.fetchone()

delete_script = '''delete from public.lubg_stg_blacklist; 
                 delete from public.lubg_stg_transactions;
                 delete from public.lubg_stg_terminals;
                 delete from public.lubg_stg_cards;
                 delete from public.lubg_stg_accounts;
                 delete from public.lubg_stg_clients;'''

cursor.execute(delete_script)
conn.commit()

df_transactions = pd.read_csv(f'transactions_{data[0]}.txt', delimiter=';',header=0)
df_transactions = df_transactions.reindex(columns=['transaction_id', 'transaction_date', 'card_num', 'oper_type', 'amount', 'oper_result', 'terminal'])
df_transactions["amount"] = df_transactions["amount"].apply(lambda x: float(x.replace(',', '.')))

df_terminals = pd.read_excel(f'terminals_file_{data[0]}.xlsx', header=0, index_col=None)
df_blacklist = pd.read_excel(f'passport_blacklist_{data[0]}.xlsx', header=0, index_col=None)
df_blacklist = df_blacklist.reindex(columns=['passport','date'])

cursor.execute("SELECT * FROM info.clients")
records = cursor.fetchall()
names = [x[0] for x in cursor.description]
df_clients = pd.DataFrame( records, columns = names)

cursor.execute("SELECT * FROM info.cards")
records = cursor.fetchall()
names = [x[0] for x in cursor.description]
df_cards = pd.DataFrame( records, columns = names)
df_cards["card_num"] = df_cards["card_num"].apply(lambda x: x.strip())

cursor.execute("SELECT * FROM info.accounts")
records = cursor.fetchall()
names = [x[0] for x in cursor.description]
df_accounts = pd.DataFrame(records, columns = names)

#Заполняем стейджинговые таблицы

cursor.executemany("INSERT INTO public.lubg_stg_blacklist(passport_num, entry_dt) VALUES(%s, %s)", df_blacklist.values.tolist())
cursor.executemany("INSERT INTO public.lubg_stg_terminals(terminal_id,terminal_type,terminal_city,terminal_address) VALUES(%s, %s, %s, %s)", df_terminals.values.tolist())
cursor.executemany("INSERT INTO public.lubg_stg_clients(client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", df_clients.values.tolist())
cursor.executemany("INSERT INTO public.lubg_stg_accounts(account_num, valid_to, client, create_dt, update_dt) VALUES(%s, %s, %s, %s, %s)", df_accounts.values.tolist())
cursor.executemany("INSERT INTO public.lubg_stg_cards(card_num, account_num, create_dt, update_dt) VALUES(%s, %s, %s, %s)", df_cards.values.tolist())
cursor.executemany("INSERT INTO public.lubg_stg_transactions(trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal) VALUES(%s, %s, %s, %s, %s, %s, %s)", df_transactions.values.tolist())
conn.commit()

# Загрузка данных в таблицы хранилища

sqls = []

dwh_terminals = 'insert into lubg_dwh_dim_terminals (terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt) select stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address, NOW(), null from lubg_stg_terminals stg left join lubg_dwh_dim_terminals tgt on stg.terminal_id = tgt.terminal_id where tgt.terminal_id is null;'
sqls.append(dwh_terminals)

dwh_clients = 'insert into lubg_dwh_dim_clients (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt) select stg.client_id, stg.last_name, stg.first_name, stg.patrinymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, NOW(), null from lubg_stg_clients stg left join lubg_dwh_dim_clients tgt on stg.client_id = tgt.client_id where tgt.client_id is null;'
sqls.append(dwh_clients)

dwh_accounts = 'insert into lubg_dwh_dim_accounts (account_num, valid_to, client, create_dt, update_dt) select stg.account_num, stg.valid_to, stg.client, NOW(), null from lubg_stg_accounts stg left join lubg_dwh_dim_accounts tgt on stg.account_num = tgt.account_num where tgt.account_num is null;'
sqls.append(dwh_accounts)

dwh_cards = 'insert into lubg_dwh_dim_cards (card_num, account_num, create_dt, update_dt) select stg.card_num, stg.account_num, NOW(), null from lubg_stg_cards stg left join lubg_dwh_dim_cards tgt on stg.card_num = tgt.card_num where tgt.card_num is null;'
sqls.append(dwh_cards)

dwh_transactions = 'insert into lubg_dwh_fact_transactions (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal) select stg.trans_id, stg.trans_date, stg.card_num, stg.oper_type, stg.amt, stg.oper_result, stg.terminal from lubg_stg_transactions stg left join lubg_dwh_fact_transactions tgt on stg.trans_id = tgt.trans_id where tgt.trans_id is null;'
sqls.append(dwh_transactions)

dwh_blacklist = 'insert into lubg_dwh_fact_blacklist (entry_dt, passport_num) select stg.entry_dt, stg.passport_num from lubg_stg_blacklist stg left join lubg_dwh_fact_blacklist tgt on tgt.passport_num = stg.passport_num where tgt.passport_num is null'
sqls.append(dwh_blacklist)

for query in sqls:
    cursor.execute(query)
    conn.commit()

cursor.execute("""update lubg_meta_dwh
set max_update_dt = NOW()
where schema_name='public' and table_name ='lubg_dwh_fact_transactions';
update lubg_meta_dwh
set max_update_dt = NOW()
where schema_name='public' and table_name ='lubg_dwh_fact_blacklist'""")
conn.commit()

#Загрузка данных в таблицы hist SCD2

sqls=[]

dwh_terminals_hist = 'insert into lubg_dwh_dim_terminals_hist  (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted) select stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address, NOW(), null, False from lubg_stg_terminals stg left join lubg_dwh_dim_terminals_hist tgt on stg.terminal_id = tgt.terminal_id where tgt.terminal_id is null'
sqls.append(dwh_terminals_hist)

dwh_clients_hist = 'insert into lubg_dwh_dim_clients_hist (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted) select stg.client_id, stg.last_name, stg.first_name, stg.patrinymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, NOW(), null, False from lubg_stg_clients stg left join lubg_dwh_dim_clients_hist tgt on stg.client_id = tgt.client_id where tgt.client_id is null'
sqls.append(dwh_clients_hist)

dwh_accounts_hist = 'insert into lubg_dwh_dim_accounts_hist (account_num, valid_to, client, effective_from, effective_to, deleted) select stg.account_num, stg.valid_to, stg.client, NOW(), null, False from lubg_stg_accounts stg left join lubg_dwh_dim_accounts_hist tgt on stg.account_num = tgt.account_num where tgt.account_num is null'
sqls.append(dwh_accounts_hist)

dwh_cards_hist = 'insert into lubg_dwh_dim_cards_hist (card_num, account_num, effective_from, effective_to, deleted) select stg.card_num, stg.account_num, NOW(), null, False from lubg_stg_cards stg left join lubg_dwh_dim_cards_hist tgt on stg.card_num = tgt.card_num where tgt.card_num is null' 
sqls.append(dwh_cards_hist)

for query in sqls:
    cursor.execute(query)
conn.commit()

# Загрузка в ДВХ обновлений в таблицах-измерениях на источниках

sqls = []

update_terminals = 'update lubg_dwh_dim_terminals set terminal_type = tmp.terminal_type, terminal_city = tmp.terminal_city, terminal_address = tmp.terminal_address,     update_dt = NOW() from (     select         stg.terminal_id,         stg.terminal_type,         stg.terminal_city,         stg.terminal_address,         tgt.create_dt,         tgt.update_dt     from lubg_stg_terminals stg     inner join lubg_dwh_dim_terminals tgt     on stg.terminal_id = tgt.terminal_id     where     ((stg.terminal_type <> tgt.terminal_type) or (stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ))     or (stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null ) or (stg.terminal_city is not null and tgt.terminal_city is null))     or (stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null ) or (stg.terminal_address is not null and tgt.terminal_address is null))) tmp where lubg_dwh_dim_terminals.terminal_id = tmp.terminal_id;'
sqls.append(update_terminals)

update_clients = 'update lubg_dwh_dim_clients set last_name = tmp.last_name, first_name = tmp.first_name, patrinymic = tmp.patrinymic,     date_of_birth = tmp.date_of_birth,     passport_num = tmp.passport_num,     phone = tmp.phone,     update_dt = NOW() from (     select         stg.client_id,         stg.last_name,         stg.first_name,         stg.patrinymic,         stg.date_of_birth,         stg.passport_num,         stg.passport_valid_to,         stg.phone,         tgt.create_dt,         tgt.update_dt     from lubg_stg_clients stg     inner join lubg_dwh_dim_clients tgt     on stg.client_id = tgt.client_id     where     ((stg.last_name <> tgt.last_name) OR     (stg.last_name IS NULL AND tgt.last_name IS NOT NULL) OR     (stg.last_name IS NOT NULL AND tgt.last_name IS NULL))     OR     ((stg.first_name <> tgt.first_name) OR     (stg.first_name IS NULL AND tgt.first_name IS NOT NULL) OR     (stg.first_name IS NOT NULL AND tgt.first_name IS NULL))     OR     ((stg.patrinymic <> tgt.patrinymic) OR     (stg.patrinymic IS NULL AND tgt.patrinymic IS NOT NULL) OR     (stg.patrinymic IS NOT NULL AND tgt.patrinymic IS NULL))     OR     ((stg.date_of_birth <> tgt.date_of_birth) OR     (stg.date_of_birth IS NULL AND tgt.date_of_birth IS NOT NULL) OR     (stg.date_of_birth IS NOT NULL AND tgt.date_of_birth IS NULL))     OR     ((stg.passport_num <> tgt.passport_num) OR     (stg.passport_num IS NULL AND tgt.passport_num IS NOT NULL) OR     (stg.passport_num IS NOT NULL AND tgt.passport_num IS NULL))     OR     ((stg.passport_valid_to <> tgt.passport_valid_to) OR     (stg.passport_valid_to IS NULL AND tgt.passport_valid_to IS NOT NULL) OR     (stg.passport_valid_to IS NOT NULL AND tgt.passport_valid_to IS NULL))     OR     ((stg.phone <> tgt.phone) OR     (stg.phone IS NULL AND tgt.phone IS NOT NULL) OR     (stg.phone IS NOT NULL AND tgt.phone IS NULL))) tmp where lubg_dwh_dim_clients.client_id = tmp.client_id;'
sqls.append(update_clients)

update_accounts = 'update lubg_dwh_dim_accounts set valid_to = tmp.valid_to, client = tmp.client, update_dt = NOW() from (     select         stg.account_num,         stg.valid_to,         stg.client,         tgt.create_dt,         tgt.update_dt     from lubg_stg_accounts stg     inner join lubg_dwh_dim_accounts tgt     on stg.account_num = tgt.account_num     where     ((stg.valid_to <> tgt.valid_to) OR     (stg.valid_to IS NULL AND tgt.valid_to IS NOT NULL) OR     (stg.valid_to IS NOT NULL AND tgt.valid_to IS NULL))     AND     ((stg.client <> tgt.client) OR     (stg.client IS NULL AND tgt.client IS NOT NULL) OR     (stg.client IS NOT NULL AND tgt.client IS NULL))) tmp where lubg_dwh_dim_accounts.account_num = tmp.account_num;'
sqls.append(update_accounts)

update_cards = 'update lubg_dwh_dim_cards set account_num = tmp.account_num, update_dt = NOW() from (select stg.card_num,         stg.account_num,         tgt.create_dt,         tgt.update_dt     from lubg_stg_cards stg     inner join lubg_dwh_dim_cards tgt     on stg.card_num = tgt.card_num     where     ((stg.account_num <> tgt.account_num) OR     (stg.account_num IS NULL AND tgt.account_num IS NOT NULL) OR     (stg.account_num IS NOT NULL AND tgt.account_num IS NULL))) tmp where lubg_dwh_dim_cards.card_num = tmp.card_num;'
sqls.append(update_cards)

for query in sqls:
    cursor.execute(query)
conn.commit()

# Загрузка в ДВХ информации об удалении данных в таблицах-измерениях на источниках

sqls = []

del_terminals = 'delete from lubg_dwh_dim_terminals where terminal_id in (     select tgt.terminal_id     from lubg_dwh_dim_terminals tgt     left join lubg_stg_terminals stg     on stg.terminal_id = tgt.terminal_id     where stg.terminal_id is null );'
sqls.append(del_terminals)

del_clients = 'delete from lubg_dwh_dim_clients where client_id in (     select tgt.client_id     from lubg_dwh_dim_clients tgt     left join lubg_stg_clients stg     on stg.client_id = tgt.client_id     where stg.client_id is null );'
sqls.append(del_clients)

del_accounts = 'delete from lubg_dwh_dim_accounts where account_num in (     select tgt.account_num     from lubg_dwh_dim_accounts tgt     left join lubg_stg_accounts stg     on stg.account_num = tgt.account_num     where stg.account_num is null );'
sqls.append(del_accounts)

del_cards = 'delete from lubg_dwh_dim_cards where card_num in (     select tgt.card_num     from lubg_dwh_dim_cards tgt     left join lubg_stg_cards stg     on stg.card_num = tgt.card_num     where stg.card_num is null );'
sqls.append(del_cards)

for query in sqls:
    cursor.execute(query)
conn.commit()

cursor.execute("""update lubg_meta_dwh
set max_update_dt = coalesce((select max(update_dt) from lubg_dwh_dim_terminals), (select max_update_dt from lubg_meta_dwh where schema_name='public' and table_name='lubg_dwh_dim_terminals'))
where schema_name='public' and table_name ='lubg_dwh_dim_terminals';
update lubg_meta_dwh
set max_update_dt = coalesce((select max(update_dt) from lubg_dwh_dim_clients), (select max_update_dt from lubg_meta_dwh where schema_name='public' and table_name='lubg_dwh_dim_clients'))
where schema_name='public' and table_name ='lubg_dwh_dim_clients';
update lubg_meta_dwh
set max_update_dt = coalesce((select max(update_dt) from lubg_dwh_dim_accounts), (select max_update_dt from lubg_meta_dwh where schema_name='public' and table_name='lubg_dwh_dim_accounts'))
where schema_name='public' and table_name ='lubg_dwh_dim_accounts';
update lubg_meta_dwh
set max_update_dt = coalesce((select max(update_dt) from lubg_dwh_dim_cards), (select max_update_dt from lubg_meta_dwh where schema_name='public' and table_name='lubg_dwh_dim_cards'))
where schema_name='public' and table_name ='lubg_dwh_dim_cards'""")
conn.commit()

# Добавление в hist таблицы информации о изменениях и удалениях данных на источнике

sql_terminals = []

update1_hist_terminal = '''update lubg_dwh_dim_terminals_hist 
set 
    terminal_type = tmp.terminal_type, 
    terminal_city = tmp.terminal_city, 
    terminal_address = tmp.terminal_address, 
    effective_to = NOW() 
from ( 
    select 
        stg.terminal_id, 
        stg.terminal_type, 
        stg.terminal_city, 
        stg.terminal_address, 
        tgt.effective_from, 
        tgt.effective_to, 
        tgt.deleted 
    from lubg_stg_terminals stg 
    inner join lubg_dwh_dim_terminals_hist tgt 
    on stg.terminal_id = tgt.terminal_id 
    where 
    ((stg.terminal_type <> tgt.terminal_type) or (stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null )) 
    or (stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null ) or (stg.terminal_city is not null and tgt.terminal_city is null)) 
    or (stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null ) or (stg.terminal_address is not null and tgt.terminal_address is null))) tmp  
where lubg_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id;'''
sql_terminals.append(update1_hist_terminal)

update2_hist_terminalll = '''insert into lubg_dwh_dim_terminals_hist (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, 
effective_to, deleted) 
select stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address, NOW(), null, False 
from lubg_stg_terminals stg 
inner join lubg_dwh_dim_terminals_hist tgt 
on stg.terminal_id = tgt.terminal_id 
where 
stg.terminal_type <> tgt.terminal_type or stg.terminal_type is null and tgt.terminal_type is not null or stg.terminal_type is not null and tgt.terminal_type is null 
or stg.terminal_city <> tgt.terminal_city or stg.terminal_city is null and tgt.terminal_city is not null  or stg.terminal_city is not null and tgt.terminal_city is null 
or stg.terminal_address <> tgt.terminal_address or stg.terminal_address is null and tgt.terminal_address is not null or stg.terminal_address is not null and tgt.terminal_address is null;'''
sql_terminals.append(update2_hist_terminalll)

del_hist_terminal = '''update lubg_dwh_dim_terminals_hist 
set 
    deleted = True 
where terminal_id in ( 
    select tgt.terminal_id 
    from lubg_dwh_dim_terminals_hist tgt 
    left join lubg_stg_terminals stg 
    on stg.terminal_id = tgt.terminal_id 
    where stg.terminal_id is null);'''
sql_terminals.append(del_hist_terminal)

for query in sql_terminals:
    cursor.execute(query)
    conn.commit()

sql_clients = []
update1_hist_clients = '''update lubg_dwh_dim_clients_hist 
set 
    last_name = tmp.last_name, 
    first_name = tmp.first_name, 
    patrinymic = tmp.patrinymic, 
    date_of_birth = tmp.date_of_birth, 
    passport_num = tmp.passport_num, 
    phone = tmp.phone, 
    effective_to = NOW() 
from ( 
    select 
        stg.client_id, 
        stg.last_name, 
        stg.first_name, 
        stg.patrinymic, 
        stg.date_of_birth, 
        stg.passport_num, 
        stg.passport_valid_to, 
        stg.phone, 
        tgt.effective_from, 
        tgt.effective_to, 
        tgt.deleted 
    from lubg_stg_clients stg 
    inner join lubg_dwh_dim_clients_hist tgt 
    on stg.client_id = tgt.client_id 
    where 
    ((stg.last_name <> tgt.last_name) OR 
    (stg.last_name IS NULL AND tgt.last_name IS NOT NULL) OR 
    (stg.last_name IS NOT NULL AND tgt.last_name IS NULL)) 
    OR 
    ((stg.first_name <> tgt.first_name) OR 
    (stg.first_name IS NULL AND tgt.first_name IS NOT NULL) OR 
    (stg.first_name IS NOT NULL AND tgt.first_name IS NULL)) 
    OR 
    ((stg.patrinymic <> tgt.patrinymic) OR 
    (stg.patrinymic IS NULL AND tgt.patrinymic IS NOT NULL) OR 
    (stg.patrinymic IS NOT NULL AND tgt.patrinymic IS NULL)) 
    OR 
    ((stg.date_of_birth <> tgt.date_of_birth) OR 
    (stg.date_of_birth IS NULL AND tgt.date_of_birth IS NOT NULL) OR 
    (stg.date_of_birth IS NOT NULL AND tgt.date_of_birth IS NULL)) 
    OR 
    ((stg.passport_num <> tgt.passport_num) OR 
    (stg.passport_num IS NULL AND tgt.passport_num IS NOT NULL) OR 
    (stg.passport_num IS NOT NULL AND tgt.passport_num IS NULL)) 
    OR 
    ((stg.passport_valid_to <> tgt.passport_valid_to) OR 
    (stg.passport_valid_to IS NULL AND tgt.passport_valid_to IS NOT NULL) OR 
    (stg.passport_valid_to IS NOT NULL AND tgt.passport_valid_to IS NULL)) 
    OR 
    ((stg.phone <> tgt.phone) OR 
    (stg.phone IS NULL AND tgt.phone IS NOT NULL) OR 
    (stg.phone IS NOT NULL AND tgt.phone IS NULL))) tmp 
where lubg_dwh_dim_clients_hist.client_id = tmp.client_id;'''
sql_clients.append(update1_hist_clients)

update2_hist_clients = '''insert into lubg_dwh_dim_clients_hist (client_id, last_name, first_name, patrinymic, 
date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted) 
select stg.client_id, stg.last_name, stg.first_name, stg.patrinymic, stg.date_of_birth, 
stg.passport_num, stg.passport_valid_to, stg.phone, NOW(), null, false 
from lubg_stg_clients stg 
inner join lubg_dwh_dim_clients tgt 
on stg.client_id = tgt.client_id 
where 
stg.last_name <> tgt.last_name OR 
stg.last_name IS NULL AND tgt.last_name IS NOT NULL OR 
stg.last_name IS NOT NULL AND tgt.last_name IS NULL 
OR stg.first_name <> tgt.first_name OR 
stg.first_name IS NULL AND tgt.first_name IS NOT null OR 
stg.first_name IS NOT NULL AND tgt.first_name IS NULL 
OR 
stg.patrinymic <> tgt.patrinymic OR 
stg.patrinymic IS NULL AND tgt.patrinymic IS NOT NULL OR 
stg.patrinymic IS NOT NULL AND tgt.patrinymic IS NULL 
OR 
stg.date_of_birth <> tgt.date_of_birth OR 
stg.date_of_birth IS NULL AND tgt.date_of_birth IS NOT null OR 
stg.date_of_birth IS NOT NULL AND tgt.date_of_birth IS NULL 
OR 
stg.passport_num <> tgt.passport_num OR 
stg.passport_num IS NULL AND tgt.passport_num IS NOT null OR 
stg.passport_num IS NOT NULL AND tgt.passport_num IS NULL 
OR 
stg.passport_valid_to <> tgt.passport_valid_to OR 
stg.passport_valid_to IS NULL AND tgt.passport_valid_to IS NOT null OR 
stg.passport_valid_to IS NOT NULL AND tgt.passport_valid_to IS NULL 
OR 
stg.phone <> tgt.phone OR 
stg.phone IS NULL AND tgt.phone IS NOT NULL OR 
stg.phone IS NOT NULL AND tgt.phone IS NULL'''
sql_clients.append(update2_hist_clients)

del_hist_clients = '''update lubg_dwh_dim_clients_hist 
set 
    deleted = true 
where client_id in ( 
    select tgt.client_id 
    from lubg_dwh_dim_clients tgt 
    left join lubg_stg_clients stg 
    on stg.client_id = tgt.client_id 
    where stg.client_id is null 
);'''
sql_clients.append(del_hist_clients)

for query in sql_terminals:
    cursor.execute(query)
    conn.commit()

sql_accounts = []

update1_hist_accounts = '''update lubg_dwh_dim_accounts_hist 
set 
    valid_to = tmp.valid_to, 
    client = tmp.client, 
    effective_to = NOW() 
from ( 
    select 
        stg.account_num, 
        stg.valid_to, 
        stg.client, 
        tgt.effective_from, 
        tgt.effective_to, 
        tgt.deleted 
    from lubg_stg_accounts stg 
    inner join lubg_dwh_dim_accounts_hist tgt 
    on stg.account_num = tgt.account_num 
    where 
    ((stg.valid_to <> tgt.valid_to) OR 
    (stg.valid_to IS NULL AND tgt.valid_to IS NOT NULL) OR 
    (stg.valid_to IS NOT NULL AND tgt.valid_to IS NULL)) 
    AND 
    ((stg.client <> tgt.client) OR 
    (stg.client IS NULL AND tgt.client IS NOT NULL) OR 
    (stg.client IS NOT NULL AND tgt.client IS NULL))) tmp 
where lubg_dwh_dim_accounts_hist.account_num = tmp.account_num;'''
sql_accounts.append(update1_hist_accounts)

update2_hist_accounts = '''insert into lubg_dwh_dim_accounts_hist (account_num, valid_to, client, 
effective_from, effective_to, deleted) 
select stg.account_num, stg.valid_to, stg.client, NOW(), null, False 
from lubg_stg_accounts stg 
inner join lubg_dwh_dim_accounts_hist tgt 
on stg.account_num = tgt.account_num 
where 
stg.valid_to <> tgt.valid_to OR 
stg.valid_to IS NULL AND tgt.valid_to IS NOT NULL OR 
stg.valid_to IS NOT NULL AND tgt.valid_to IS NULL 
AND 
stg.client <> tgt.client OR 
stg.client IS NULL AND tgt.client IS NOT NULL OR 
stg.client IS NOT NULL AND tgt.client IS NULL;'''
sql_accounts.append(update2_hist_accounts)

del_hist_accounts = '''update lubg_dwh_dim_accounts_hist 
set 
    deleted = true 
where account_num in ( 
    select tgt.account_num 
    from lubg_dwh_dim_accounts tgt 
    left join lubg_stg_accounts stg 
    on stg.account_num = tgt.account_num 
    where stg.account_num is null 
);'''
sql_accounts.append(del_hist_accounts)

for query in sql_terminals:
    cursor.execute(query)
    conn.commit()

sql_cards = []

update1_hist_cards = '''update lubg_dwh_dim_cards_hist 
set 
    account_num = tmp.account_num, 
    effective_to = NOW() 
from ( 
    select 
        stg.card_num, 
        stg.account_num, 
        tgt.effective_from, 
        tgt.effective_to, 
        tgt.deleted 
    from lubg_stg_cards stg 
    inner join lubg_dwh_dim_cards_hist tgt 
    on stg.card_num = tgt.card_num 
    where 
    ((stg.account_num <> tgt.account_num) OR 
    (stg.account_num IS NULL AND tgt.account_num IS NOT NULL) OR 
    (stg.account_num IS NOT NULL AND tgt.account_num IS NULL))) tmp 
where lubg_dwh_dim_cards_hist.card_num = tmp.card_num;'''
sql_cards.append(update1_hist_cards)

update2_hist_cards = '''insert into lubg_dwh_dim_cards_hist (card_num, account_num, effective_from, effective_to, deleted) 
select stg.card_num, stg.account_num, NOW(), null, False 
from lubg_stg_cards stg 
inner join lubg_dwh_dim_cards_hist tgt 
on stg.card_num = tgt.card_num 
where 
stg.account_num <> tgt.account_num OR 
stg.account_num IS NULL AND tgt.account_num IS NOT null OR 
stg.account_num IS NOT NULL AND tgt.account_num IS NULL'''
sql_cards.append(update2_hist_cards)

del_hist_cards = '''update lubg_dwh_dim_cards_hist 
set 
    deleted = true 
where card_num in ( 
    select tgt.card_num 
    from lubg_dwh_dim_cards tgt 
    left join lubg_stg_cards stg 
    on stg.card_num = tgt.card_num 
    where stg.card_num is null 
);'''
sql_cards.append(del_hist_cards)

for query in sql_terminals:
    cursor.execute(query)
    conn.commit()

cursor.execute("""update lubg_meta_dwh
set max_update_dt = NOW()
where schema_name='public' and table_name ='lubg_dwh_dim_terminals_hist';
update lubg_meta_dwh
set max_update_dt = NOW()
where schema_name='public' and table_name ='lubg_dwh_dim_clients_hist';
update lubg_meta_dwh
set max_update_dt = NOW()
where schema_name='public' and table_name ='lubg_dwh_dim_accounts_hist';
update lubg_meta_dwh
set max_update_dt = NOW()
where schema_name='public' and table_name ='lubg_dwh_dim_cards_hist'""")
conn.commit()

joint_table = '''create table lubg_joint_table_tmp as 
select tr.trans_id,
        tr.trans_date,
        tr.amt,
        tr.oper_result,
        cli.passport_valid_to,
        cli.passport_num,
        concat(cli.last_name,' ',cli.first_name,' ',cli.patrinymic) as fio,
        cli.phone as phone,
        term.terminal_city,
        acc.account_num,
        acc.valid_to,
        cli.client_id
from lubg_dwh_fact_transactions tr
inner join lubg_dwh_dim_cards cards
on cards.card_num = tr.card_num
inner join lubg_dwh_dim_terminals term
on tr.terminal = term.terminal_id
inner join lubg_dwh_dim_accounts acc
on cards.account_num = acc.account_num
inner join lubg_dwh_dim_clients cli
on acc.client = cli.client_id'''

cursor.execute(joint_table)
conn.commit()

# Поиск и добавление в отчет мошеннических операций

frauds = []

fraud1 = '''insert into lubg_rep_fraud
select trans_date event_dt, passport_num as passport, fio, phone, 
'заблокированный или просроченный паспорт' as event_type, CURRENT_DATE as report_dt from lubg_joint_table_tmp
where passport_valid_to < trans_date or
passport_num in (select passport_num from lubg_dwh_fact_blacklist)'''
frauds.append(fraud1)

fraud2 = '''insert into lubg_rep_fraud
select trans_date event_dt, passport_num as passport, fio, phone, 
'недействующий договор' as event_type, CURRENT_DATE as report_dt from lubg_joint_table_tmp
where valid_to < trans_date'''
frauds.append(fraud2)

fraud3 = '''insert into lubg_rep_fraud
with cte as (
select count(distinct terminal_city), date_trunc('hour', trans_date) as trans_date, client_id
from lubg_joint_table_tmp
group by date_trunc('hour', trans_date), client_id
having count(distinct terminal_city) > 1)
select distinct tmp.trans_date event_dt, passport_num as passport, fio, phone, 
'операции в разных городах' as event_type, CURRENT_DATE as report_dt from lubg_joint_table_tmp tmp
inner join cte
on tmp.client_id = cte.client_id
where date_trunc('hour', tmp.trans_date) = cte.trans_date 
'''
frauds.append(fraud3)

fraud4 = '''insert into lubg_rep_fraud
with cte as (select client_id, 
        oper_result, 
        trans_date, 
        row_number() over(partition by client_id order by trans_date asc) as rk, 
        amt,
        (trans_date - LAG(trans_date) over(partition by client_id order by trans_date asc)) +
        (trans_date - LAG(trans_date,2) over(partition by client_id order by trans_date asc)) as minutes_diff,
        LAG(amt) over(partition by client_id order by trans_date asc) as amt_previos,
        LAG(amt,2) over(partition by client_id order by trans_date asc) as amt_second_previous,
        LAG(oper_result) over(partition by client_id order by trans_date asc) as previos_status,
        LAG(oper_result, 2) over(partition by client_id order by trans_date asc) as second_previos_status
from lubg_joint_table_tmp),
cte1 as (
select*from cte
where second_previos_status = 'REJECT' 
and previos_status = 'REJECT' 
and oper_result = 'SUCCESS'
and extract(minute from minutes_diff) <= 20
and amt_second_previous > amt_previos
and amt_previos > amt)
select tmp.trans_date event_dt, passport_num as passport, fio, phone, 
'попытка подбора суммы' as event_type, CURRENT_DATE as report_dt from lubg_joint_table_tmp tmp
inner join cte1
on tmp.client_id = cte1.client_id and tmp.trans_date = cte1.trans_date'''
frauds.append(fraud4)

for query in frauds:
    cursor.execute(query)
conn.commit()

cursor.execute('drop table lubg_joint_table_tmp')
conn.commit()

os.rename(f'transactions_{data[0]}.txt', f'archive/transactions_{data[0]}.txt.backup')
os.rename(f'terminals_{data[0]}.xlsx', f'archive/terminals_{data[0]}.xslx.backup')
os.rename(f'passport_blacklist_{data[0]}.xlsx', f'archive/passport_blacklist_{data[0]}.xlsx.backup')

next_date = datetime.strptime(data[0], '%d%m%Y')
next_date =  next_date + timedelta(days=1)
next_date = next_date.strftime('%d%m%Y')

cursor.execute(f"update lubg_last_report_date set dt = '{next_date}'")
conn.commit()

cursor.close()
conn.close()

