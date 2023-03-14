import token as token

api_version = '3.17'
test_server = 'SERVERNAME.DOMAN'
test_server_url = 'https://domainhost'
prod_server_url = 'https://domainhost/'
site_name = ''
site_url = ''
user_domain = ("company.domain\\" )


tableau_config = {
    'tableau_prod': {
        'server': test_server_url,
        'api_version': api_version,
        'personal_access_token_name': token.personal_token_name,
        'personal_access_token_secret': token.personal_token_password,
        'site_name': site_name,
        'site_url': site_url,
        'cache_buster': '',
        'temp_dir': ''
    }
}