import logging as log

import tableau_api_lib as t
from config import INO_Tableau_Config as i
from tableau_api_lib.utils.querying import (get_group_users_dataframe,
                                            get_subscriptions_dataframe)

# Define Variables Needed
# Tableau ID for MDR Dashboard View
ino_view_id = ""
# Tableau ID for MDR Group
ino_group_id = ""
# Testing Schedule
schedule_id = ""
# Testing Schedule message
my_message = "Rest API Testing"

# Create Functions to execute

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

def users_pull(conn, group, license_type):
    """API Request to pull user data from Tableau based on a group id and a license type. License type is either "Unlicensed" or "Licensed".
       Any other input for that variable will raise a ValueError.
       All arguments are required for function to execute.
    Args:
        conn (PAT passthrough): allows API requests to connect to Tableau
        group (string): Tableau ID defining which group users are pulled from.
        license_type (string): string variable to determine what user licenses are pulled. License type is either "Unlicensed" or "Licensed",

    Raises:
        ValueError: Raises error is license_type argument is not Licensed or Unlicensed.

    Returns:
        licensed_users: Dataframe of users with a licensed site role (Viewer, Creator, Explorer etc.). Contains columns for user_id, user name, and siteRole.
        unlicensed_users: Dataframe of users with an unlicensed site role. Contains columns for user_id, user name, and siteRole.
    """   
    g_user_df = get_group_users_dataframe(conn, group_id=group)
    g_user_df.rename(columns={"id" : "user_id"}, inplace= True)
    license_type = license_type.title()
    if license_type == "Unlicensed":
        unlicensed_cond = g_user_df["siteRole"] == "Unlicensed"
        unlicensed_users = g_user_df[unlicensed_cond]
        return unlicensed_users
        # unlicensed_list = g_user_df["user_id"][unlicensed_cond].tolist()
        # return unlicensed_list
    elif license_type == "Licensed":
        licensed_cond = g_user_df["siteRole"] != "Unlicensed"
        licensed_users = g_user_df[licensed_cond]
        return licensed_users
        # licensed_list = g_user_df["id"][licensed_cond].tolist()
        # return licensed_list
    elif license_type != "Unlicensed" | license_type != "Licensed":
        raise ValueError("Unrecognized license type. Please input either Licensed or Unlicensed")
    

def subscriptions_pull(conn, view) :
    """API Request to pull subscriptions to a specific Tableau view. Requires a view id to be passed
       in order to pull subscriptions. Returns a dataframe of subscriptions.

    Args:
        conn (PAT passthrough): allows API requests to connect to Tableau
        view (string): Tableau ID defining which view subscriptions are pulled from.

    Returns:
        final_subs: Dataframe of subscriptions to the view id passed as an argument. Includes subscription id, user_id, user name.
    """ 
    subs_df = get_subscriptions_dataframe(conn)
    view_filter = subs_df["content_id"] == view
    subs_df = subs_df[view_filter]
    subs_df.rename(columns={"id" : "subscription_id"}, inplace= True)
    final_subs = subs_df[["subscription_id","user_id","user_name"]]
    return final_subs


def find_users_missing_subscription(licensed, subscrips):
    """Takes a user dataframe and subscription dataframe, joins them on user_id, and drops any column where a user already has a subscription id.
       For any user missing a subscription id, their user id is stored into a new dataframe and gets returned as a list of strings.

    Args:
        licensed (DataFrame): Dataframe of licensed users. Required argument that is used to determine which users are missing a subscription to a specific view
        subscrips (DataFrame): Dataframe of all subscriptions to a specific view. Required argument that is used to determine if users are not subscribed to selected view .

    Returns:
        missing_subs: List of Tableau user_id strings.
    """    
    new = licensed.join(subscrips.set_index("user_id"), on="user_id", how="left")
    missing_subs = new[new["subscription_id"].isnull()]
    missing_subs = missing_subs["user_id"].tolist()
    return missing_subs

def find_subscription_ids_unlicensed_users(user_list, subscription_df):
    """Requires a dataframe of unlicensed users and a dataframe of all subscriptions to the previously indicated view.
       The function will join both dataframes on user_id and output a list of the subscription ids for all unlicensed users.

    Args:
        user_list (DataFrame): Dataframe of unlicensed users. Required argument that is used to determine which users need subscriptions deleted.
        subscription_df (DataFrame): Dataframe of subscriptions to the view id passed as an argument. Includes subscription id, user_id, user name.    subscription_df (_type_): _description_

    Returns:
        _type_: _description_
    """    
    delete_ids = user_list.join(subscription_df.set_index("user_id"), on="user_id", how="right")
    delete_ids = delete_ids["subscription_id"].tolist()
    return delete_ids

def create_user_subscription(conn, scheduleid, contentid, contenttype, sub_subject, my_message, userid):
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
    """    
    create = conn.create_subscription(schedule_id= scheduleid, content_id= contentid, content_type= contenttype,
                                       subscription_subject = sub_subject, message = my_message, user_id = userid,
                                       attach_pdf_flag=True, attach_image_flag=True)
    create
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
    """
    delete = conn.delete_subscription(user_id)
    status_code = delete.status_code
    if status_code == 204:
        print("Subscrtiption Deleted.")
    elif status_code != 204:
        print("Deletion Status code not returned. Something might have gone wrong.")
    return 

def main():
    # Step 1: Sign into Tableau
    conn = sign_in()


    # Step 2: Pull unlicensed users in the MDR Group
    unlicensed_users_tb = users_pull(conn, ino_group_id, "Unlicensed")

    
    # Step 3: Pull licensed users in the MDR Group
    licensed_users_tb = users_pull(conn, ino_group_id, "Licensed")

    
    # Step 4: Pull Subscriptions to the MDR Dashboard View
    subscriptions_mdr_dataframe = subscriptions_pull(conn, ino_view_id)


    # Step 5: For each Unlicensed User subscribed to the MDR Dashboard, delete the subscription.
    delete_subscription_ids = find_subscription_ids_unlicensed_users(unlicensed_users_tb,subscriptions_mdr_dataframe)
    delete_subscription_ids_len = len(delete_subscription_ids_len)
    if delete_subscription_ids_len == 0:
        print("There are no unlicensed Users to delete subscriptions for.")
        return
    else:
      [delete_subscription_ids(x) for x in delete_subscription_ids]  
    print("Unlicensed user subscriptions deleted.")


    # Step 6: For each licensed User not in the Subscription DataFrame
    missing_subs = find_users_missing_subscription(licensed_users_tb, subscriptions_mdr_dataframe)
    missing_subs_len = len(missing_subs)
    if missing_subs_len == 0:
        print("There are no Users needing subscription creation.")
        return
    else:
        [create_user_subscription(conn = conn, scheduleid = schedule_id, contentid = ino_view_id, userid = x, 
                             contenttype= 'view', my_message = my_message ,
                             sub_subject= 'Subscription REST API Test'
                                    ) for x in missing_subs]
    
    # Sign out
    sign_out(conn)


if __name__ == "__main__":
    main()
