import requests
import tweepy
import time
from shroomdk import ShroomDK

## Insert your ShroomDK API Key Here
sdk = ShroomDK("ShroomDK-API-KEY")

## Twitter API Keys
consumer_key = "consumer_key"
consumer_secret = "consumer_secret"
access_token = "access_token"
access_token_secret = "access_token_secret"

## Twitter oAuth2 to gain API access
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

## Run Query to Get data in Shroom DK 
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

## Sort the results and prep the tweet body 
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
      payload = f"{address} withdrew {token_amount} {currency} (${amount_usd}) of staking rewards at {blockts} UTC"
    elif action == 'redelegate': 
      payload = f"{address} redelegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC"
    elif action == 'pool_exited': 
      payload = f"{address} withdrew {token_amount} {currency} (${amount_usd}) from a liquidity pool at {blockts} UTC"
    elif action == 'swap': 
      payload = f"{address} swapped {token_amount} {currency} (${amount_usd}) for {to_token_amount} {to_currency} at {blockts} UTC"
    elif action == 'pool_joined': 
      payload = f"{address} added {token_amount} {currency} (${amount_usd}) to a liquidity pool at {blockts} UTC"
    elif action == 'transfer': 
      payload = f"{address} transferred {token_amount} {currency} (${amount_usd}) to {receiver} at {blockts} UTC"
    elif action == 'delegate': 
      payload = f"{address} delegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC"
    elif action == 'claim': 
      payload = f"{address} claimed {token_amount} {currency} (${amount_usd}) at {blockts} UTC"
    else:
      payload = f"{address} undelegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC"

    api.update_status(payload)
    print('Done!')
