####
# This script houses all variables for connection to Tableau, san PAT (Personal Access Token).
# Functions will be defined for connecting to Tableau and accessing data.

import token as token

api_version = "3.17"
domain = ""
test_server_url = "https://tableau-test." + domain + ".com"
prod_server_url = "https://tableau." + domain + ".com/"
site_name = ""
site_url = ""
user_domain = ""


tableau_config = {
    "tableau_dev": {
        "server": test_server_url,
        "api_version": api_version,
        "personal_access_token_name": token.personal_token_name,
        "personal_access_token_secret": token.personal_token_password,
        "site_name": site_name,
        "site_url": site_url,
        "cache_buster": "",
        "temp_dir": "",
    }
}
