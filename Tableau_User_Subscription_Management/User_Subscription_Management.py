import config.tableau_config as i
import pandas as pd
import tableau_api_lib as t
from sqlalchemy import create_engine, text, URL
from tableau_api_lib.utils.querying import (
    get_group_users_dataframe,
    get_groups_dataframe,
    get_projects_dataframe,
    get_schedules_dataframe,
    get_subscriptions_dataframe,
    get_users_dataframe,
    get_views_dataframe,
    get_workbooks_dataframe,
)

# Define Variables Needed
## Connection Establish
user_name = ""
user_pass = ""

connection_url = URL.create(
    "mssql+pyodbc",
    username=user_name,
    password=user_pass,
    host="",
    port="1433",
    database="Analytics",
    query={"driver": "ODBC Driver 17 for SQL Server"},
)

e = create_engine(connection_url)

cxn = e.connect()
cxn2 = e.raw_connection()

## Variables Initialized

sql_schema = "Tableau"
sql_uspPubs = "Tableau.uspMaintainPublications_TEST"
sql_view = "View_TEST"
sql_project = "Project_TEST"
sql_group = "Group_TEST"
sql_schedule = "Schedule_TEST"
sql_workbook = "Workbook_TEST"
sql_tableau_publication = "Publication_TEST"
sql_tableau_group = "GroupSubscription_TEST"

## Defined Functions


def sign_in():
    """Signs into Tableau TEST using the config module to send PAT as a secure sign-in method.

    Returns:
        conn: secure server connection attachment for executing Tableau queries and edits.
    """
    conn = t.TableauServerConnection(i.tableau_config)
    try:
        conn.sign_in()
        if conn.sign_in().status_code == 200:
            print("PAT Login Successful.")
            return conn
    except:
        "Connection Failed."


def sign_out(conn):
    """Officially disconnects from the Tableau server.

    Args:
        conn (PAT passthrough): Tableau connection attachment
    """
    conn.sign_out()
    user = print("Sign out Successful.")
    return user


def view_path_name_strip(viewpathname):
    """
    The function takes a string as an argument and returns the same string with everything after the last '>' character removed.

    Args:
        viewpathname (string): Store the view path name
    Returns:
        final_string (string): The name of the view
    """

    mystring = viewpathname
    rev_string = mystring[::-1]
    char_index = int(rev_string.find(">"))
    final_string = rev_string[:char_index]
    final_string = final_string[::-1]
    return final_string


def fetch_tableau_data(conn):
    """
    The fetch_tableau_data function is used to fetch data from Tableau Server.
    The function takes a connection object as an argument and returns a tuple of pandas DataFrames containing the following:
        1) Views - A DataFrame of all views on the server, including their IDs and names.
        2) Projects - A DataFrame of all projects on the server, including their IDs and names.
        3) Groups - A DataFrame of all groups on the server, including their IDs and names.
        4) Schedules - A DataFrame of all schedules on the server, including their IDs and names (and other information).
        5) Subscriptions - A DataFrame of all subscriptions on the server, including their IDs and content types (and other information).

    Args:
        conn (PAT passthrough): Connect to the Tableau server
    Returns:
        df_tuple (tuple): A tuple of dataframes pulled from the Tableau server
    """
    t_views = get_views_dataframe(conn)
    t_projects = get_projects_dataframe(conn)
    t_groups = get_groups_dataframe(conn)
    t_schedules = get_schedules_dataframe(conn)
    t_subscriptions = get_subscriptions_dataframe(conn)
    t_workbook = get_workbooks_dataframe(conn)

    t_schedules_final = t_schedules.drop(
        [
            "createdAt",
            "updatedAt",
            "type",
            "frequency",
            "nextRunAt",
            "state",
            "priority",
        ],
        axis=1,
    )
    t_schedules_final = t_schedules_final.rename(
        columns={"id": "ScheduleID", "name": "ScheduleName"}
    )

    t_groups_final = t_groups.rename(columns={"id": "GroupID", "name": "GroupName"})
    t_groups_final.drop(["domain", "import"], axis=1, inplace=True)
    t_groups_final.sort_values(by="GroupName", inplace=True)

    # isolate projectID
    t_views["ProjectID"] = t_views["project"].apply(lambda x: x.get("id"))
    # Trim Views table to necessary columns
    t_views_final = t_views[["id", "name", "ProjectID"]]
    t_views_final = t_views_final.rename(columns={"id": "ViewID", "name": "ViewName"})

    t_projects_final = t_projects.sort_values(by=["parentProjectId"])
    t_projects_final.drop(
        [
            "owner",
            "description",
            "createdAt",
            "updatedAt",
            "contentPermissions",
            "controllingPermissionsProjectId",
        ],
        axis=1,
        inplace=True,
    )
    t_projects_final.rename(
        columns={
            "id": "ProjectID",
            "name": "ProjectName",
            "parentProjectId": "ParentProjectID",
        },
        inplace=True,
    )

    t_workbook["ProjectID"] = t_workbook["project"].apply(lambda x: x.get("id"))
    t_workbook["ProjectName"] = t_workbook["project"].apply(lambda x: x.get("name"))
    t_workbook.rename(
        columns={
            "id": "WorkbookID",
            "name": "WorkbookName",
            "description": "Description",
            "createdAt": "CreatedAt",
            "updatedAt": "UpdatedAt",
            "size": "Size",
            "defaultViewId": "DefaultViewID",
        },
        inplace=True,
    )
    t_workbook_final = t_workbook[
        [
            "WorkbookID",
            "WorkbookName",
            "ProjectID",
            "ProjectName",
            "Description",
            "DefaultViewID",
            "Size",
            "CreatedAt",
            "UpdatedAt",
        ]
    ]

    df_tuple = (
        t_views_final,
        t_projects_final,
        t_groups_final,
        t_schedules_final,
        t_subscriptions,
        t_workbook_final,
    )
    return df_tuple


def tableau_to_sql(dataFrame, table_name):
    """
    The tableau_to_sql function takes a pandas dataframe and the name of a table in the Tableau schema.
    It then writes that dataframe to that table, replacing any existing rows.

    Args:
        dataFrame (DataFrame): The dataframe that you want to upload
        table_name (string): Name of the SQL Table to upload the dataframe
    Returns:
        Nothing
    """

    dataFrame.to_sql(
        name=table_name,
        schema="Tableau",
        con=e,
        if_exists="replace",
        index=False,
        chunksize=None,
    )
    return


def sql_data_maintain(stored_procedure) -> None:
    with e.connect() as connection:
        try:
            function = connection.execute(text(stored_procedure))
            results = function.fetchone()

        except connection.ProgrammingError:
            print("The stored procedure could not be found.")
            connection.rollback()
        if results[0] == "Complete with errors":
            print("Stored Procedure completed with errors.")
        if results[0] == "Complete":
            print("Stored Procedure Executed.")
        else:
            print("Stored Procedure did not execute.")
            connection.rollback()
    return


def pull_sql_data(tablename, schema, connection):
    """
    The pull_sql_data function takes in a table name, schema and connection object.
    It then uses the pandas read_sql_table function to pull data from the specified table into a pandas DataFrame.
    The function returns this DataFrame.

    Args:
        tablename (string): Specify the table name that you want to pull data from
        schema (string): Specify the schema of the table you want to pull data from
        connection (SQL engine): Connection to the SQL Server
    Returns:
        df (DataFrame): DataFrame of data based on the parameters

    """

    df = pd.read_sql_table(table_name=tablename, schema=schema, con=connection)
    return df


def subscriptions_pull(conn):
    """API Request to pull subscriptions to all Tableau views. Returns a dataframe consisting of a Subscription_id, View_id, User_id, and user_name.
    This DataFrame allows the program to delete subscriptions for users later on based on their site roles.

    Args:
        conn (PAT passthrough): allows API requests to connect to Tableau
        view (string): Tableau ID defining which view subscriptions are pulled from.

    Returns:
        final_subs (DataFrame): Dataframe of subscriptions Tableau views. Includes Subscription_id, View_id, User_id, and user_name.
    """
    subs_df = get_subscriptions_dataframe(conn)
    subs_df.rename(
        columns={"id": "subscription_id", "content_id": "view_id"}, inplace=True
    )
    final_subs = subs_df[["subscription_id", "view_id", "user_id", "user_name"]]
    return final_subs


def create_user_subscription(
    conn,
    scheduleid,
    contentid,
    contenttype,
    sub_subject,
    my_message,
    userid,
    attach_pdf_flag,
    attach_image_flag,
    pdf_page_orientation,
    pdf_page_size,
    send_view_if_empty_flag,
):
    """API Request to create a new subscription to Tableau content. Takes arguments to fulfill the subscription requirements.
       Once a subscription creation request is pushed, the function will return printed results based on HTTP response codes.

    Args:
        conn (PAT passthrough): allows API requests to connect to Tableau
        scheduleid (string): Tableau ID of schedule the subscription will be sent to user.
        contentid (string): Tableau ID of the content being subscribed to by user.
        contenttype (string): Text input of the content type. Content types: View, Dashboard
        sub_subject (string): Text input of the subscriptions subject.
        my_message (string): Text input of the message sent via email when a subscription is pushed by the schedule.
        userid (string): Tableau ID of the user getting a subscription created.
        attach_pdf_flag (bool): True/False on whether PDFs should be attached to the subscription.
        attach_image_flag (bool): True/False on whether images should be attached to the subscription.
        pdf_page_orientation (string): Designates the orientation of PDF pages attached to the subscription.
        pdf_page_size (string): Designates the PDF page size attached to the subscription.
        send_view_if_empty_flag (bool): True/False on whether the view should be sent if it is empty on refresh.

    Returns:
        Nothing
    """
    create = conn.create_subscription(
        schedule_id=scheduleid,
        content_id=contentid,
        content_type=contenttype,
        subscription_subject=sub_subject,
        message=my_message,
        user_id=userid,
        attach_pdf_flag=attach_pdf_flag,
        attach_image_flag=attach_image_flag,
        pdf_page_orientation=pdf_page_orientation,
        pdf_page_size=pdf_page_size,
        send_view_if_empty_flag=send_view_if_empty_flag,
    )
    results = create.status_code
    if results == 201:
        print("Subscription Created.")
    elif results == 200:
        print("Subscription Existed.")
    else:
        print("Error. Subscription not created.")
    return


def delete_user_subscription(conn, user_id):
    """API request to delete subscriptions for users. Requires at least one user_id.

    Args:
        conn (PAT passthrough): allows API requests to connect to Tableau
        userid (string): Tableau ID of the user getting a subscription created.
    Returns:
        Nothing
    """
    delete = conn.delete_subscription(user_id)
    status_code = delete.status_code
    if status_code == 204:
        print("Subscrtiption Deleted.")
    elif status_code != 204:
        print("Deletion Status code not returned. Something might have gone wrong.")
    return


def create_missing_subscriptions(input_dataframe, tableau_subscriptions, connection):
    """
    The create_missing_subscriptions function will create subscriptions for users in a group that do not have an existing subscription to the view.

    Args:
        input_dataframe (DataFrame): Pass the dataframe that contains the subscription information
        tableau_subscriptions (DataFrame): Filter the subscriptions dataframe to only include subscriptions for the viewid being processed
        connection (PAT Passthrough): Connect to the tableau server
    Returns:
        Nothing
    """

    for ViewID in input_dataframe["ViewID"].unique():

        get_path = input_dataframe[input_dataframe["ViewID"] == ViewID]
        # get_path = get_path["ViewPathAndName"].to_string(index=False)
        # get_path = view_path_name_strip(get_path)
        # get_view = get_view["ViewName"][0]

        print("Reviewing View: {}".format(get_path))

        #       GET subscriptions FOR ViewPathAndName
        subscriptions_filtered = tableau_subscriptions[
            tableau_subscriptions["content_id"] == ViewID
        ]

        #       FOR EACH GroupName IN Analytics.Tableau.GroupSubscription_TEST {
        for groupID in input_dataframe["GroupID"].unique():
            t_users = get_group_users_dataframe(connection, groupID)
            t_users = t_users.rename(columns={"id": "UserID"})
            licensed_users = t_users[t_users["siteRole"] != "Unlicensed"]
            licensed_users = licensed_users.drop(
                columns=["lastLogin", "locale", "language", "externalAuthUserId"]
            )

            user_exceptions = input_dataframe["UserExceptions"].to_list()

            if len(user_exceptions) > 0:
                print("User Exceptions found: {}".format(user_exceptions))
                t_users = t_users[~t_users["fullName"].isin(user_exceptions)]
            else:
                print("No user exceptions found.")

            get_missing_subscriptions = licensed_users.join(
                subscriptions_filtered.set_index("user_id"),
                on="UserID",
                how="left",
                lsuffix="_l",
                rsuffix="_tab",
            )
            missing_condition = get_missing_subscriptions["content_id"].isnull()
            missing_subscriptions = get_missing_subscriptions[missing_condition]

            if len(missing_subscriptions) == 0:

                print("There are no user subscriptions to create.")

            else:

                for row in missing_subscriptions.itertuples():

                    scrp_subject = input_dataframe["SubscriptionSubject"].to_string(
                        index=False
                    )
                    scrp_id = input_dataframe["ScheduleID"].to_string(index=False)
                    scrp_message = input_dataframe["SubscriptionMessage"].to_string(
                        index=False
                    )
                    scrp_username = row.fullName
                    scrp_user = row.UserID
                    scrp_image = input_dataframe["AttachImage"].to_string(index=False)
                    scrp_pdf = input_dataframe["AttachPDF"].to_string(index=False)
                    scrp_pdf_orientation = input_dataframe["PageOrientation"].to_string(
                        index=False
                    )
                    scrp_pdf_size = input_dataframe["PageSizeOption"].to_string(
                        index=False
                    )
                    scrp_empty_flag = input_dataframe["SendIfViewEmpty"].to_string(
                        index=False
                    )

                    print(
                        "Creating subscription for {} to {}".format(
                            scrp_username, scrp_subject
                        )
                    )

                    create_user_subscription(
                        conn=connection,
                        scheduleid=scrp_id,
                        contentid=ViewID,
                        contenttype="view",
                        sub_subject=scrp_subject,
                        my_message=scrp_message,
                        userid=scrp_user,
                        attach_image_flag=scrp_image,
                        attach_pdf_flag=scrp_pdf,
                        pdf_page_orientation=scrp_pdf_orientation,
                        pdf_page_size=scrp_pdf_size,
                        send_view_if_empty_flag=scrp_empty_flag,
                    )
    return


def delete_unlicensed_user_subscriptions(tableau_subscriptions, connection):
    """
    The delete_unlicensed_user_subscriptions function deletes subscriptions for unlicensed users.
        It takes two arguments:
            1) tableau_subscriptions - a dataframe containing all of the subscriptions in Tableau Server.
            2) connection - a connection to the Tableau Server database.

    Args:
        tableau_subscriptions (DataFrame): Pass the dataframe containing all of the subscriptions
        connection (PAT Passthrough): Connect to the tableau server
    Returns:
        Nothing
    """

    t_users = get_users_dataframe(connection)

    t_users["id"] = t_users["id"].astype(str)
    all_unlicensed_users = t_users[t_users["siteRole"] == "Unlicensed"]

    deletable_subscriptions = tableau_subscriptions[
        tableau_subscriptions["user_id"].isin(all_unlicensed_users["id"])
    ]

    for row in deletable_subscriptions.itertuples():
        delete_users = row.user_name
        delete_users = delete_users.replace(".", " ")
        subscription_subject = row.subject
        subscription_id = row.id

        print(
            "Deleting subscription to '{}' for unlicensed user: {}".format(
                subscription_subject, delete_users
            )
        )
        delete_user_subscription(connection, subscription_id)
        print("Subscription deleted.")
        return


## EXECUTING SCRIPT
def main():
    """
    The main function is the entry point for this script. It calls all other functions in order to accomplish its goal of maintaining Tableau subscriptions.

    :return: Nothing, so the if statement is always true
    """

    conn = sign_in()
    # Pull all Tableau data
    tableau_dataframes = fetch_tableau_data(conn)

    # Create variable for each dataframe extracted by previous function
    tableau_views = tableau_dataframes[0]
    tableau_projects = tableau_dataframes[1]
    tableau_groups = tableau_dataframes[2]
    tableau_schedules = tableau_dataframes[3]
    tableau_subscriptions = tableau_dataframes[4]
    tableau_workbooks = tableau_dataframes[5]

    # Send organized Tableau data to SQL tables
    tableau_to_sql(tableau_views, sql_view)
    tableau_to_sql(tableau_projects, sql_project)
    tableau_to_sql(tableau_groups, sql_group)
    tableau_to_sql(tableau_schedules, sql_schedule)
    tableau_to_sql(tableau_workbooks, sql_workbook)

    # Execute stored procedure for Tableau data
    sql_data_maintain(sql_uspPubs)

    # Pull maintained data from SQL into separate DataFrames

    sql_tableau_publication = pull_sql_data(
        tablename=sql_tableau_publication, schema=sql_schema, connection=cxn
    )
    sql_tableau_group_subscription = pull_sql_data(
        tablename=sql_tableau_group, schema=sql_schema, connection=cxn
    )

    sql_tableau_data = sql_tableau_publication.join(
        sql_tableau_group_subscription.set_index("PublicationID"),
        on="PublicationID",
        how="right",
    )

    sql_tableau_data["ViewID"] = sql_tableau_data["ViewID"].astype(str)
    sql_tableau_data["ScheduleID"] = sql_tableau_data["ScheduleID"].astype(str)
    sql_tableau_data["GroupID"] = sql_tableau_data["GroupID"].astype(str)

    create_missing_subscriptions(sql_tableau_data, tableau_subscriptions, conn)

    delete_unlicensed_user_subscriptions(tableau_subscriptions, conn)

    return


if __name__ == "__main__":
    main()
