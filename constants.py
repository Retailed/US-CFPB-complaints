# information on how to connect to postgres database
DB_DRIVER = 'postgresql'
DB_NAME = 'cfpb'
DB_HOST = 'localhost'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'

# address of the archive with the csv, which should be saved
COMPLAINTS_CSV_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"

# name of csv file with complaints
CSV_FILE_NAME = 'complaints.csv'

# amount of rows, loaded from csv at once, as it's quite big
CHUNKSIZE_FOR_PARTIAL_LOADING = 2 * 10 ** 5

# names of companies to draw a complaints graph of
COMPANY_NAME1 = 'EQUIFAX, INC.'
COMPANY_NAME2 = 'TRANSUNION INTERMEDIATE HOLDINGS, INC.'