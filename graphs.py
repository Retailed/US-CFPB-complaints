import pandas as pd
from sqlalchemy import create_engine, exc, engine

import plotly.graph_objects as go

from constants import DB_DRIVER, DB_NAME, DB_HOST, DB_USER, DB_PASSWORD, \
    COMPANY_NAME1, COMPANY_NAME2

CONNECTION = engine.URL.create(drivername=DB_DRIVER,
                               host=DB_HOST,
                               username=DB_USER,
                               password=DB_PASSWORD,
                               database=DB_NAME)

def count_complaints_sum_over_dates(df):
    '''
    Counts how sum of received complaints rises over the dates.
    The issues marked as removed ones are subtracted.

    :param df: Dataframe with columns 'date_received' and marked 'is_removal'
    :return: Dataframe with columns 'date' and 'issue_sum'
    '''

    # get all unique dates from this dataframe
    dates_arr = df['date_received'].unique()
    dates_arr.sort()

    issue_sum = pd.DataFrame(columns=['date', 'issue_sum'])
    sum = 0
    for date in dates_arr:
        issues_this_date = df[df['date_received'] == date]
        sum_series = (issues_this_date[issues_this_date['is_removal'] == False].count() -
                      issues_this_date[issues_this_date['is_removal'] == True].count())
        sum += sum_series['date_received']
        issue_sum = issue_sum.append({'date': date,
                                      'issue_sum': sum},
                                     ignore_index=True)

    return issue_sum


def draw_complaints_over_time(df1, df2):
    '''
    Draws a graph of complaints sum for COMPANY_NAME1 and COMPANY_NAME2
    :param df1: Dataframe with complaints for COMPANY_NAME1 with columns 'date' and 'issue_sum'
    :param df2: Dataframe with complaints for COMPANY_NAME2 with columns 'date' and 'issue_sum'
    :return: None
    '''
    fig = go.Figure()
    fig.add_scatter(x=df1['date'],
                    y=df1['issue_sum'],
                    mode='lines',
                    name=COMPANY_NAME1)
    fig.add_scatter(x=df2['date'],
                    y=df2['issue_sum'],
                    mode='lines',
                    name=COMPANY_NAME2)
    fig.update_layout(
        height=800,
        title_text='Complaints over time'
    )
    fig.show()


def merge_normal_and_deleted_complaints(complaints_created_df, complaints_deleted_df):
    '''
    Merges dataframe with created complaints with dataframe of removed complaints into one Dataframe,
    marking removed complaints in new column 'is_removal'.
    :param complaints_created_df: Dataframe with data on created complaints
    :param complaints_deleted_df: Dataframe with data on removed complaints
    :return: Dataframe, consisting of parameter dataframes merged, column is_removal marks
    rows from complaints_deleted_df
    '''
    merged_df = complaints_created_df
    merged_df['is_removal'] = False
    merged_df = merged_df.append(complaints_deleted_df).fillna(True)
    return merged_df


def complaints_for_two_companies(engine):
    '''
    Draws graphs counting number of complaints for two different companies over time,
    whose names are set as constants COMPANY_NAME1 and COMPANY_NAME2

    :param engine: SQLAlchemy connection to database
    :return: None
    '''

    request = "select distinct complaint_id, date_received " \
              "from complaints where company = '{}'" \
              "order by date_received, complaint_id".format(COMPANY_NAME1)
    company1_complaints_df = pd.read_sql(request, con=engine)

    request = "select distinct complaint_id, date_received " \
              "from complaints where company = '{}'" \
              "order by date_received, complaint_id".format(COMPANY_NAME2)
    company2_complaints_df = pd.read_sql(request, con=engine)

    # now we need to get issues, which were deleted in the end
    # they can be detected by their complaint_id
    request = '''
    select date_received, complaint_id, update_stamp
    from complaints
    where company is NULL and complaint_id in (
    select complaint_id
    from complaints
    where company = '{}'
    )
    '''.format(COMPANY_NAME1)
    company1_removed_complaints_df = pd.read_sql(request, con=engine)

    request = '''
    select date_received, complaint_id, update_stamp
    from complaints
    where company is NULL and complaint_id in (
    select complaint_id
    from complaints
    where company = '{}'
    )
    '''.format(COMPANY_NAME2)
    company2_removed_complaints_df = pd.read_sql(request, con=engine)

    # merging removals and normal additions and amends into one dataframe
    company1_merged_df = merge_normal_and_deleted_complaints(company1_complaints_df,
                                                            company1_removed_complaints_df)
    company2_merged_df = merge_normal_and_deleted_complaints(company2_complaints_df,
                                                               company2_removed_complaints_df)

    # count how many issues appeared or disappeared on that day
    company1_complaints_over_time = count_complaints_sum_over_dates(company1_merged_df)
    company2_complaints_over_time = count_complaints_sum_over_dates(company2_merged_df)

    draw_complaints_over_time(company1_complaints_over_time, company2_complaints_over_time)


def draw_daily_updates(df):
    '''
    Draws a graph of daily updates for data in df, separating created and updated complaints
    :param df: Dataframe with columns 'date', 'issues_created' and 'issues_updated'
    :return: None
    '''
    fig1 = go.Figure()
    fig1.add_scatter(x=df['date'],
                     y=df['issues_created'],
                     name='Complaints created daily')

    fig1.add_scatter(x=df['date'],
                     y=df['issues_updated'],
                     name='Complaints updated daily')

    fig1.update_layout(
        height=800,
        title_text='Updates each day'
    )
    fig1.show()


def complaints_per_day(engine):
    '''
    Draw a graphs counting updates each day, separating new complaints and modified complaints
    (deleted complaints are removed from both new complaints and modified complaints)
    :param engine: SQLAlchemy connection to database
    :return: None
    '''

    # if there's more than one entry in complaints table,
    # that means there were modifications or removal
    request = '''
    select complaint_id, date_received, count(*) as amount
    from complaints
    where not (product is NULL)
    group by complaint_id, date_received
    order by date_received;
    '''
    complaints_amount_by_id = pd.read_sql(request, con=engine)

    # get ids of complaints which were removed in the end
    request = '''
    select complaint_id, date_received
    from complaints
    where product is NULL
    order by date_received;
    '''
    complaints_removed = pd.read_sql(request, con=engine)

    unique_dates = complaints_amount_by_id['date_received'].unique()

    updates_by_date = pd.DataFrame()
    for date in unique_dates:
        complaints_this_date = complaints_amount_by_id[
            complaints_amount_by_id['date_received'] == date
            ]
        complaints_removed_this_date = complaints_removed[
            complaints_removed['date_received'] == date
        ]
        amount_of_complaints_removed = len(complaints_removed_this_date.index)
        issues_created = len(complaints_this_date.index) - amount_of_complaints_removed

        issues_updated_this_date = complaints_this_date[complaints_this_date['amount'] > 1]
        # removals are also modifications in some sort
        # but the decision is to also exclude them from modification
        issues_updated = len(issues_updated_this_date.index)

        updates_by_date = updates_by_date.append({'date': date,
                                                  'issues_created': issues_created,
                                                  'issues_updated': issues_updated},
                                                 ignore_index=True)

    draw_daily_updates(updates_by_date)


if __name__ == '__main__':
    try:
        engine = create_engine(CONNECTION)

        print('Drawing a graph of complaints over time for 2 companies')
        complaints_for_two_companies(engine)

        print('Drawing a graph of created and updated complaints per day')
        complaints_per_day(engine)
    except exc.OperationalError as op_err:
        print('! Failed to connect to the database')
        print(op_err.orig)
    except exc.ProgrammingError as pr_err:
        print('! Failed to execute sql script')
        print(pr_err.orig)
    except Exception as some_err:
        print('! Something went wrong')
        print(some_err)
