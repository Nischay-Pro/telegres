import argparse
import os
import psycopg
import pickle
import json

def main():
    parser = argparse.ArgumentParser(description='Migrate pickle persistence to postgreSQL.')
    parser.add_argument('--config', '-c', help='Path to config file.', required=True)
    parser.add_argument('--pickle', '-p', help='Path to pickle file.', required=True)

    args = parser.parse_args()

    if not os.path.isfile(args.config):
        print('[-] Config file not found.')
        exit(1)
    
    if not os.path.isfile(args.pickle):
        print('[-] Pickle file not found.')
        exit(1)
    
    print('[+] Migrating pickle file to postgreSQL.')

    print('[+] Reading config file.')
    config = json.load(open(args.config))

    if config['database']['type'] != 'postgres':
        print('[-] Only postgreSQL is supported.')
        exit(1)
    
    postgres_host = config['database']['host']
    postgres_port = config['database']['port']
    postgres_user = config['database']['username']
    postgres_password = config['database']['password']
    postgres_db = config['database']['database']
    postgres_schema = config['database']['schema']
    skip_null = config['database']['skip_null']

    print('[+] Connecting to postgreSQL.')
    
    try:
        conn = psycopg.connect(host=postgres_host, port=postgres_port, user=postgres_user, password=postgres_password, dbname=postgres_db)
        cur = conn.cursor()
    except psycopg.OperationalError as e:
        print('[-] Could not connect to postgreSQL.')
        print('[-] Error: {}'.format(e))
        exit(1)

    print('[+] Connected to postgreSQL.')

    print('[+] Reading pickle file.')
    
    try:
        pickle_data = pickle.load(open(args.pickle, 'rb'))
    except pickle.UnpicklingError as e:
        print('[-] Could not read pickle file.')
        print('[-] Error: {}'.format(e))
        exit(1)

    print('[+] Read pickle file.')

    print('[+] Migrating pickle data to postgreSQL.')

    print('[+] Creating schema.')
    cur.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(postgres_schema))
    print('[+] Created schema.')

    print('[+] Creating timestamp triggers.')
    sql_function = """
        CREATE OR REPLACE FUNCTION trigger_set_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
                NEW.updated_at = now(); 
                RETURN NEW;
            ELSE
                RETURN OLD;
            END IF;
        END;
        $$ language 'plpgsql';
        """

    cur.execute(sql_function)
    print('[+] Created timestamp triggers.')

    bot_keys = tuple(pickle_data.keys())

    print('[+] Creating tables.')

    for bot_key in bot_keys:
        if len(pickle_data[bot_key]) > 0:
            table_name = bot_key.replace("_data", "")
            sql_command = (
                "CREATE TABLE IF NOT EXISTS {0}.telegram_{1} "
                "(id BIGINT NOT NULL PRIMARY KEY, data jsonb NOT NULL, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());".format(
                    postgres_schema, table_name
                )
            )
            cur.execute(sql_command)
            sql_trigger = (
                "CREATE TRIGGER set_timestamp BEFORE UPDATE ON {0}.telegram_{1} FOR EACH ROW "
                "EXECUTE PROCEDURE trigger_set_timestamp();".format(postgres_schema, table_name)
            )
            cur.execute(sql_trigger)
            print('[+] Created table {}.'.format(table_name))
            print('[+] Detected {0} entries for {1}.'.format(len(pickle_data[bot_key]), table_name))
            print('[+] Inserting {0} entries for {1}.'.format(len(pickle_data[bot_key]), table_name))
            for entry in pickle_data[bot_key]:
                sql_command = (
                    "INSERT INTO {0}.telegram_{1} (id, data) VALUES (%s, %s);".format(
                        postgres_schema, table_name
                    )
                )
                if skip_null and len(pickle_data[bot_key][entry]) == 0:
                    continue
                cur.execute(sql_command, (entry, json.dumps(pickle_data[bot_key][entry])))
            print('[+] Migrated {0}.'.format(table_name))
    
    print('[+] Committing changes.')
    conn.commit()
    print('[+] Committed changes.')
    conn.close()
            

if __name__ == "__main__":
    main()