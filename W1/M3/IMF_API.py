# before start, install sdmx

# import libraries
import  sdmx
# from msal import PublicClientApplication

# retrieve data
IMF_DATA = sdmx.Client('IMF_DATA')
data_msg = IMF_DATA.data('CPI', key='USA+CAN.CPI.CP01.IX.M', params={'startPeriod': 2018})

# convert to pandas
cpi_df = sdmx.to_pandas(data_msg)
print(cpi_df.head())

"""
connecting to portal as an authenticated user


# parameter values for authorization and data requests
client_id = '446ce2fa-88b1-436c-b8e6-94491ca4f6fb'
authority = 'https://imfprdb2c.b2clogin.com/imfprdb2c.onmicrosoft.com/b2c_1a_signin_aad_simple_user_journey/'
scope = 'https://imfprdb2c.onmicrosoft.com/4042e178-3e2f-4ff9-ac38-1276c901c13d/iData.Login'

# authorize and retrieve access token
app = PublicClientApplication(client_id,authority=authority)
token = None
token = app.acquire_token_interactive(scopes=[scope])

# define header for a request
header = {'Authorization': f"{token['token_type']} {token['access_token']}"}

# retrieve data
IMF_DATA = sdmx.Client('IMF_DATA')
data_msg = IMF_DATA.data('CPI', key='USA+CAN.CPI.CP01.IX.M', params={'startPeriod': 2018}, headers=header)

# convert to pandas
cpi_df = sdmx.to_pandas(data_msg)
print(cpi_df.head())

"""