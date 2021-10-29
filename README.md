### Documentation
- [Task](#task)
- [How to use](#how-to-use)
- [Solution details](#solution-details)


### Task

This test task represents a simple example of the data we work with and the type of problems you will 
be facing during real-life data development. 

The public database in question was created after the financial crisis of 2008 to store and make public
consumer complaints, submitted to US Consumer Financial Protection Bureau (CFPB) by individuals unhappy 
about the products they have been sold and services provided by US retail banks.
The data is freely available, among other channels, at
https://www.consumerfinance.gov/data-research/consumer-complaints. 

Please study various ways of loading that data and regular updates programmatically, and choose one. 
You should also choose how you would store data on your end, for which we suggest a relational database(preferably PostgreSQL).

Starting from scratch, your program should load the full history of complaints since inception. 

When the data is loaded (or when it has already been created in a previous program run), 
you will need to load only last month's worth of complaints to update / amend the database 
(here, we assume that we will run the program regularly).

It is expected that in addition to new records which have been added since the last run, 
you might encounter changes in the records you already have on your end. 
We propose the following versioning system: have a separate update_stamp column in addition to the columns 
provided by CFPB, where you will store the actual time you got and save the record. 
For new records, just add them to your database. For updated records, add another row with a new 
(obviously newer) update_stamp.

For deleted rows, we suggest you create a new row filling all columns (apart from complaint_id 
and date_recieved (the keys that we suggest for the table) and update_stamp ) with NaNs, 
to indicate that the data is no longer there. For the records which have not been changed since 
the last update nothing needs to be changed or updated (i.e. update_stamp). 

Note, that you will have to work with the resulting table, which might have duplicated complaint_id's 
with different update_stamps, to construct the latest version of the data you have on your end, 
for the purpose of comparing records with CFPB.

You may assume that the number of amends will be low compared to the overall number of records. 
You will also notice that since we load only last month's worth of complaints in the incremental regime, 
we are blind to changes in older records, but assume that this is an acceptable compromise.

Finally we need to be able to estimate some data properties. 
Draw a graph counting updates each day (separate amends and new additions), 
and another counting number of complaints for two different companies over time.

### How to use

1. Change values, used for connection to the Postgres database, in constants.py. 
   To create a new database, a Docker container can be used. For doing this, run `docker-compose up -d`
   in command line from the main folder. If using this Docker image, constants don't need to be changed.
   
2. Install requirements with `pip3 install -r requirements.txt`

3. For loading information into the database, either creating a table from scratch or updating already existing one,
   run `main.py`

4. For drawing graphs for complaints over time for 2 companies and daily creation and updates on issues
   run `graphs.py`
   
### Solution details

- No matter when the complaint is changed or removed, the date in `date_received` field
does not change. So these changes are referred to changes, happened at `date_received`.
  Knowing that the program will run regularly, it might make sense to graph them as something
  happened at `update_stamp` day.
  
- Entries describing deleted complaints can be found by checking NULL values in `product`
column, as it is the object of complaint, and without it the complaint can't be submitted.
  
- When drawing a graph over updates each day, deleted complaints are not counted as updates,
and their amount is subtracted from created complaints.