from requests_oauthlib import OAuth1Session
from shroomdk import ShroomDK
import os
import json
import time

os.environ["CONSUMER_KEY"] = "twitter_consumer_key"
os.environ["CONSUMER_SECRET"] = "twitter_consumer_secret"
sdk = ShroomDK("ShroomDK_API_KEY")

consumer_key = os.environ.get("CONSUMER_KEY")
consumer_secret = os.environ.get("CONSUMER_SECRET")

# Run SQL Query using ShroomDK 
sql = f"""
SELECT 
    block_timestamp,
    date_trunc('hour', block_timestamp) as time, 
    delegator_address as address, 
    action, 
    amount / POW(10, s.decimal) as token_amount, 
    s.currency, 
    t.project_name,
    s.amount / POW(10, s.decimal) * price as amount_usd, 
    NULL as to_currency, 
    NULL as to_project_name,
    NULL as to_token_amount, 
    NULL AS receiver
FROM OSMOSIS.CORE.FACT_STAKING s 
INNER JOIN OSMOSIS.CORE.EZ_PRICES p
ON date_trunc('hour', s.block_timestamp) = p.recorded_hour
AND s.currency = p.currency
INNER JOIN osmosis.core.dim_tokens t
ON s.currency = t.address
WHERE time :: date >= current_date() - 1 
AND amount_usd >= 10000
UNION 
SELECT 
   block_timestamp,
   date_trunc('hour', block_timestamp) as time, 
   liquidity_provider_address as address, 
   action, 
   amount / POW(10, l.decimal) as token_amount, 
   l.currency, 
   t.project_name, 
   l.amount / POW(10, l.decimal) * price as amount_usd, 
   NULL as to_currency,
   NULL as to_project_name,
   NULL as to_token_amount, 
   NULL AS receiver
FROM OSMOSIS.CORE.FACT_LIQUIDITY_PROVIDER_ACTIONS l
INNER JOIN OSMOSIS.CORE.EZ_PRICES p
ON date_trunc('hour', l.block_timestamp) = p.recorded_hour
AND l.currency = p.currency
INNER JOIN osmosis.core.dim_tokens t
ON l.currency = t.address
WHERE time :: date >= current_date() - 1
AND amount_usd >= 10000
UNION
SELECT 
   block_timestamp,
   date_trunc('hour', block_timestamp) as time, 
   locker_address as address, 
   'unlock' as action, 
   amount / POW(10, l.decimal) as token_amount, 
   l.currency, 
   t.project_name,
   l.amount / POW(10, l.decimal) * price as amount_usd, 
   NULL as to_currency, 
   NULL as to_project_name,
   NULL as to_token_amount, 
   NULL AS receiver
FROM OSMOSIS.CORE.FACT_LOCKED_LIQUIDITY_ACTIONS l
INNER JOIN OSMOSIS.CORE.EZ_PRICES p
ON date_trunc('hour', l.block_timestamp) = p.recorded_hour
AND l.currency = p.currency
INNER JOIN osmosis.core.dim_tokens t
ON l.currency = t.address
WHERE time :: date >= current_date() - 1
AND amount_usd >= 10000
UNION 
SELECT 
   block_timestamp,
   date_trunc('hour', block_timestamp) as time, 
   delegator_address as address, 
   action, 
   amount / POW(10, l.decimal) as token_amount, 
   l.currency, 
   t.project_name, 
   l.amount / POW(10, l.decimal) * price as amount_usd, 
   NULL as to_currency, 
   NULL as to_project_name,
   NULL as to_token_amount, 
   NULL AS receiver
FROM OSMOSIS.CORE.FACT_STAKING_REWARDS l
INNER JOIN OSMOSIS.CORE.EZ_PRICES p
ON date_trunc('hour', l.block_timestamp) = p.recorded_hour
AND l.currency = p.currency
INNER JOIN osmosis.core.dim_tokens t
ON l.currency = t.address
WHERE time :: date >= current_date() - 1
AND amount_usd >= 10000
UNION 
SELECT 
   block_timestamp,
   date_trunc('hour', block_timestamp) as time, 
   delegator_address as address, 
   action, 
   amount / POW(10, l.decimal) as token_amount, 
   l.currency, 
   t.project_name,
   l.amount / POW(10, l.decimal) * price as amount_usd, 
   NULL as to_currency, 
   NULL as to_project_amount,
   NULL as to_token_amount, 
   NULL AS receiver
FROM OSMOSIS.CORE.FACT_SUPERFLUID_STAKING l
INNER JOIN OSMOSIS.CORE.EZ_PRICES p
ON date_trunc('hour', l.block_timestamp) = p.recorded_hour
AND l.currency = p.currency
INNER JOIN osmosis.core.dim_tokens t
ON l.currency = t.address
WHERE time :: date >= current_date() - 1
AND amount_usd >= 10000
UNION 
SELECT 
   block_timestamp,
   date_trunc('hour', block_timestamp) as time, 
   trader as address, 
   'swap' as action, 
   from_amount / POW(10, l.from_decimal) as token_amount, 
   l.from_currency, 
   t.project_name,
   l.from_amount / POW(10, l.from_decimal) * price as amount_usd, 
   l.to_currency as to_currency, 
   tt.project_name as to_project_name,
   l.to_amount / POW(10, l.to_decimal) as to_token_amount, 
   NULL AS receiver
FROM OSMOSIS.CORE.FACT_SWAPS l
INNER JOIN OSMOSIS.CORE.EZ_PRICES p
ON date_trunc('hour', l.block_timestamp) = p.recorded_hour
AND l.from_currency = p.currency
INNER JOIN osmosis.core.dim_tokens t
ON l.from_currency = t.address
INNER JOIN osmosis.core.dim_tokens tt
ON l.to_currency = tt.address
WHERE time :: date >= current_date() - 30
AND amount_usd >= 10000
UNION 
SELECT 
   block_timestamp,
   date_trunc('hour', block_timestamp) as time, 
   sender as address, 
   'transfer' as action, 
   amount / POW(10, l.decimal) as token_amount, 
   l.currency, 
   t.project_name,
   l.amount / POW(10, l.decimal) * price as amount_usd, 
   NULL as to_currency, 
   NULL as to_project_name,
   NULL as to_token_amount, 
   receiver 
FROM OSMOSIS.CORE.FACT_TRANSFERS l
INNER JOIN OSMOSIS.CORE.EZ_PRICES p
ON date_trunc('hour', l.block_timestamp) = p.recorded_hour
AND l.currency = p.currency
INNER JOIN osmosis.core.dim_tokens t
ON l.currency = t.address
WHERE time :: date >= current_date() - 30
AND amount_usd >= 10000
"""

query_result_set = sdk.query(sql)

# Get request token
request_token_url = "https://api.twitter.com/oauth/request_token?oauth_callback=oob&x_auth_access_type=write"
oauth = OAuth1Session(consumer_key, client_secret=consumer_secret)

try:
    fetch_response = oauth.fetch_request_token(request_token_url)
except ValueError:
    print(
        "There may have been an issue with the consumer_key or consumer_secret you entered."
    )

resource_owner_key = fetch_response.get("oauth_token")
resource_owner_secret = fetch_response.get("oauth_token_secret")
print("Got OAuth token: %s" % resource_owner_key)

# Get authorization
base_authorization_url = "https://api.twitter.com/oauth/authorize"
authorization_url = oauth.authorization_url(base_authorization_url)
print("Please go here and authorize: %s" % authorization_url)
verifier = input("Paste the PIN here: ")

# Get the access token
access_token_url = "https://api.twitter.com/oauth/access_token"
oauth = OAuth1Session(
    consumer_key,
    client_secret=consumer_secret,
    resource_owner_key=resource_owner_key,
    resource_owner_secret=resource_owner_secret,
    verifier=verifier,
)
oauth_tokens = oauth.fetch_access_token(access_token_url)

access_token = oauth_tokens["oauth_token"]
access_token_secret = oauth_tokens["oauth_token_secret"]

# Make the request
oauth = OAuth1Session(
    consumer_key,
    client_secret=consumer_secret,
    resource_owner_key=access_token,
    resource_owner_secret=access_token_secret,
)

# Be sure to add replace the text of the with the text you wish to Tweet. You can also add parameters to post polls, quote Tweets, Tweet with reply settings, and Tweet to Super Followers in addition to other features.
for record in query_result_set.records: 
    blockts = record['block_timestamp']
    address = record['address']
    action = record['action']
    token_amount = record['token_amount']
    currency = record['project_name']
    amount_usd = record['amount_usd']
    to_currency = record['to_project_name']
    to_token_amount = record['to_token_amount']
    receiver = record['receiver']
    time.sleep(10)
    if action == 'withdraw_rewards': 
      payload = {"text": f"{address} withdrew {token_amount} {currency} (${amount_usd}) of staking rewards at {blockts} UTC"}
    elif action == 'redelegate': 
      payload = {"text": f"{address} redelegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC"}
    elif action == 'pool_exited': 
      payload = {"text": f"{address} withdrew {token_amount} {currency} (${amount_usd}) from a liquidity pool at {blockts} UTC"}
    elif action == 'swap': 
      payload = {"text": f"{address} swapped {token_amount} {currency} (${amount_usd}) for {to_token_amount} {to_currency} at {blockts} UTC"}
    elif action == 'pool_joined': 
      payload = {"text": f"{address} added {token_amount} {currency} (${amount_usd}) to a liquidity pool at {blockts} UTC"}
    elif action == 'transfer': 
      payload = {"text": f"{address} transferred {token_amount} {currency} (${amount_usd}) to {receiver} at {blockts} UTC"}
    elif action == 'delegate': 
      payload = {"text": f"{address} delegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC"}
    elif action == 'claim': 
      payload = {"text": f"{address} claimed {token_amount} {currency} (${amount_usd}) at {blockts} UTC"}
    else:
      payload = {"text": f"{address} undelegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC"}

    # Making the request
    response = oauth.post(
        "https://api.twitter.com/2/tweets",
        json=payload,
    )

    if response.status_code != 201:
        raise Exception(
            "Request returned an error: {} {}".format(response.status_code, response.text)
        )

    print("Response code: {}".format(response.status_code))

    # Saving the response as JSON
    json_response = response.json()
    print(json.dumps(json_response, indent=4, sort_keys=True))
