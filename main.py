import pandas as pd

import zipfile
from io import BytesIO
import requests

import os

import sqlalchemy_utils as sql_utils
from sqlalchemy import create_engine, engine, exc
import psycopg2

from datetime import datetime as dt
import dateutil.relativedelta

from constants import DB_DRIVER, DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, \
    COMPLAINTS_CSV_URL, CSV_FILE_NAME, CHUNKSIZE_FOR_PARTIAL_LOADING

CONNECTION = engine.URL.create(drivername=DB_DRIVER,
                               host=DB_HOST,
                               username=DB_USER,
                               password=DB_PASSWORD,
                               database=DB_NAME)


def create_new_column_names(column_names):
    '''
    Changes column names for "niser" naming - lowercase, underscores
    instead of spaces, removing question marks.
    :param column_names: a list of column names to change
    :return: a dictionary, where keys are old column names, items are changed names
    '''

    new_names = {}
    for elem in column_names:
        new_names[elem] = elem.replace(' ', '_').lower().replace('?', '')

    return new_names


def check_table_existence(conn, table_name):
    '''
    Checks whether the table under table_name exists in the database/
    :param conn: psycopg2 connection to database
    :param table_name: name of the table, which existence is checked
    :return: bool, True if such table exists
    '''
    with conn.cursor() as cur:
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
        return cur.fetchone()[0]


def get_entries_received_after_chosen_datetime(conn, table_name, date_time):
    '''
    Fetches from chosen database entries, whose 'date_received' field 
    is bigger than date_time value (means it happened after chosen datetime)
    :param conn: SQLAlchemy connection to the database
    :param table_name: name of the database table for querying
    :param date_time: datetime type value to
    :return: Dataframe with rows satisfying this condition
    '''
    string_datetime = date_time.strftime("%Y-%m-%d %H:%M:%S")
    request = "select * from {} where date_received > '{}'".format(table_name, string_datetime)

    return pd.read_sql(request, con=conn)


def load_and_extract_zip(url):
    '''
    Downloads and unpacks ZIP-file in main folder.
    :param url: URL, where ZIP-file is stored
    :return: list of filenames from this ZIP-file
    '''
    try:
        with requests.get(url) as response:
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                zip_file.extractall()
                return zip_file.namelist()
    except requests.exceptions.ConnectionError as con_err:
        print('! Can\'t connect to address and download csv to work with')
    except requests.exceptions.HTTPError as http_err:
        print('! Can\'t download csv to work with - the unsuccessful response status')


def prepare_table_creation_string(column_infos):
    '''
    Used for creating query for saving data from complaints into the table with the same name.
    :param column_infos: dictionary with pairs '<column name>': <column type>'
    :return: string for querying, with columns from column_infos, 'update_stamp' column
    and 'complaint_id' and 'update_stamp' as compound primary key.
    '''
    creation_str = 'CREATE TABLE complaints ( '
    for key, value in column_infos.items():
        creation_str += f'"{key}" {value},\n'

    creation_str += '''
    update_stamp TIMESTAMP,
    PRIMARY KEY (complaint_id, update_stamp));'''

    return creation_str


def create_main_table(conn, column_names_df):
    '''
    Creates complaints table with columns from column_names_df + 'update_stamp'.
    Compound primary key is 'complaint_id' and 'update_stamp'
    :param conn: psycopg2 connection to the database
    :param column_names_df: Dataframe which data will be saved to the table
    :return: None
    '''
    column_infos = {}

    for column in column_names_df:
        if pd.api.types.is_integer_dtype(column_names_df[column]):
            column_infos[column] = 'INTEGER'
        elif pd.api.types.is_datetime64_any_dtype(column_names_df[column]):
            column_infos[column] = 'TIMESTAMP'
        else:
            column_infos[column] = 'TEXT'

    creation_str = prepare_table_creation_string(column_infos)

    with conn.cursor() as cur:
        cur.execute(creation_str)
    conn.commit()


def leave_only_last_update_in_df(df):
    '''
    Removes from the dataframe old entries, returning only latest update for each complaint_id.
    The latest update has the latest 'update_stamp'.
    :param df: Dataframe with 'complaint_id' and 'update_stamp' fields,
    whose data will be modified
    :return: Dataframe df with only latest update on each complaint_id
    '''
    # save unique values
    latest_updates = df.drop_duplicates(subset=['complaint_id'],
                                        keep=False)

    # get the non-unique values
    non_unique_ids = df[df['complaint_id'].duplicated()]
    non_unique_ids_set = (set(non_unique_ids['complaint_id'].tolist()))

    # from non-unique values choose the ones with latest timestamp
    for id in non_unique_ids_set:
        this_id = df[df['complaint_id'] == id]
        latest_updates = latest_updates.append(this_id[this_id['update_stamp'] == this_id['update_stamp'].max()])

    return latest_updates


def load_full_data(conn):
    # get the column names and change them to suit database-like naming
    column_names = pd.read_csv(CSV_FILE_NAME, nrows=1).columns.tolist()
    new_column_names = create_new_column_names(column_names)

    # choose the ones with "Date" in name
    contain_dates = [col_name for col_name in new_column_names if 'date' in new_column_names[col_name]]

    # read some data from csv to find out their types
    some_data_for_type_detection = pd.read_csv(CSV_FILE_NAME,
                                               parse_dates=contain_dates,
                                               nrows=1000).rename(columns=new_column_names)

    # and create the table itself
    create_main_table(conn, some_data_for_type_detection)

    # loading the csv in chunks, as it's quite huge to process in one go
    engine = create_engine(CONNECTION)

    print('Saving full history of complaints to the datatable. This process takes quite a while...')
    with pd.read_csv(CSV_FILE_NAME,
                     chunksize=CHUNKSIZE_FOR_PARTIAL_LOADING,
                     parse_dates=contain_dates) as reader:
        for chunk in reader:
            chunk = chunk.rename(columns=new_column_names)

            # adding update_time column
            chunk['update_stamp'] = pd.to_datetime('now')

            chunk.to_sql('complaints',
                         con=engine,
                         if_exists='append',
                         index=False)


def save_new_entries(engine, new_entries, entries_from_db):
    '''
    Saves to the database complaints, which don't exist there, checking by 'complaint_id'
    :param engine: SQLAlchemy connection to the database
    :param new_entries: Dataframe with newly loaded information
    :param entries_from_db: Dataframe with information, loaded from the database
    :return: None
    '''
    # save entries which don't exist in db
    new_entries['already_saved'] = new_entries['complaint_id'].isin(entries_from_db['complaint_id'])
    df_to_save = new_entries.loc[new_entries['already_saved'] == False].drop(['already_saved'], axis=1)
    df_to_save['update_stamp'] = pd.to_datetime('now')
    df_to_save.to_sql('complaints',
                      con=engine,
                      if_exists='append',
                      index=False)


def delete_disappeared_entries(engine, new_entries, entries_from_db):
    '''
    Adds to the database information on complaints, which were deleted in newer revision.
    Deleted entries only have 'complaint_id', 'date_received' and 'update_stamp' field values,
    all other fields are NULL.
    :param engine: SQLAlchemy connection to the database
    :param new_entries: Dataframe with newly loaded information
    :param entries_from_db: Dataframe with information, loaded from the database
    :return: None
    '''


    entries_from_db['was_removed'] = ~entries_from_db['complaint_id'].isin(new_entries['complaint_id'])
    df_to_save = entries_from_db.loc[entries_from_db['was_removed'] == True].drop(['was_removed'], axis=1)
    # some of them are already deleted in database, we need to remove them

    df_to_save = df_to_save.dropna(subset=['product'])
    df_to_save = df_to_save[['complaint_id', 'date_received']]
    df_to_save['update_stamp'] = pd.to_datetime('now')
    df_to_save.to_sql('complaints',
                      con=engine,
                      if_exists='append',
                      index=False)


def update_changed_entries(engine, new_entries, entries_from_db):
    '''
    Updates complaints, which were not deleted, but whose information changed.
    :param engine: SQLAlchemy connection to the database
    :param new_entries: Dataframe with newly loaded information
    :param entries_from_db: Dataframe with information, loaded from the database
    :return: None
    '''
    # to find rows that changed let's remove unnecessary columns and append tables
    new_entries = new_entries[new_entries['already_saved'] != False].drop(['already_saved'], axis=1)
    new_entries['update_stamp'] = pd.to_datetime('now')
    df_to_save = new_entries.append(entries_from_db[entries_from_db['was_removed'] != True]
                                    .drop(['was_removed'], axis=1))

    # if the entry didn't change, now there are 2 rows with the same information, except for 'update_stamp'
    # so removing all duplicates will make only entries with updates stay
    df_to_save = df_to_save.loc[:, df_to_save.columns != 'update_stamp'].drop_duplicates(keep=False)

    # ids of these rows are needed for update
    updated_row_ids = set(df_to_save['complaint_id'].values.tolist())
    df_to_save = new_entries[new_entries['complaint_id'].isin(updated_row_ids)]
    df_to_save.to_sql('complaints',
                      con=engine,
                      if_exists='append',
                      index=False)


def update_months_data():
    # get the column names and change them to suit database-like naming
    column_names = pd.read_csv(CSV_FILE_NAME, nrows=1).columns.tolist()
    new_column_names = create_new_column_names(column_names)

    # choose the ones with "Date" in name
    contain_dates = [col_name for col_name in new_column_names if 'date' in new_column_names[col_name]]

    # get entries in range we're interested in
    engine = create_engine(CONNECTION)
    new_entries = pd.DataFrame()

    # getting entries received month ago
    date_month_ago = dt.now() + dateutil.relativedelta.relativedelta(months=-1)
    with pd.read_csv(CSV_FILE_NAME,
                     chunksize=CHUNKSIZE_FOR_PARTIAL_LOADING,
                     parse_dates=contain_dates) as reader:
        for chunk in reader:
            chunk = chunk.rename(columns=new_column_names)
            new_entries = new_entries.append(chunk.loc[chunk['date_received'] > date_month_ago])

    # search for the same entries in database
    entries_from_db = get_entries_received_after_chosen_datetime(engine, 'complaints', date_month_ago)
    entries_from_db = leave_only_last_update_in_df(entries_from_db)

    print('Adding new complaints to the table')
    save_new_entries(engine, new_entries, entries_from_db)

    print('Marking deleted complaints in the table')
    delete_disappeared_entries(engine, new_entries, entries_from_db)

    print('Updating changed complaints in the table')
    update_changed_entries(engine, new_entries, entries_from_db)


if __name__ == '__main__':
    try:
        print('Downloading data to load or update the table')
        files_from_zip = load_and_extract_zip(COMPLAINTS_CSV_URL)
        assert CSV_FILE_NAME in files_from_zip, '! csv file naming has changed, please update constants'

        # create database, if needed
        if not (sql_utils.database_exists(CONNECTION)):
            print('Creating database to store data')
            sql_utils.create_database(CONNECTION)

        # check if table exists
        conn = psycopg2.connect(dbname=DB_NAME,
                                host=DB_HOST,
                                user=DB_USER,
                                password=DB_PASSWORD)

        if not check_table_existence(conn, 'complaints'):
            print('Loading full history of complaints')
            load_full_data(conn)
            print('All data loaded to complaints table!')

        else:
            print('Updating last month\'s complaints info')
            update_months_data()
            print('All data for last month is updated!')

        # delete csv from folder
        for file in files_from_zip:
            os.remove(file)
    except exc.OperationalError as op_err:
        print('! Failed to connect to the database')
        print(op_err.orig)
    except exc.ProgrammingError as pr_err:
        print('! Failed to execute sql script')
        print(pr_err.orig)
    except psycopg2.OperationalError as op_err:
        print('! Failed to connect to the database')
        print(op_err.pgerror)
    except Exception as e:
        print(e)
