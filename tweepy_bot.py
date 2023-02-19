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
    tx_id,
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
   tx_id, 
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
   tx_id,
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
   tx_id,
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
   tx_id,
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
   tx_id,
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
   tx_id,
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
for record in data: 
    tx_id = record['TX_ID']
    link = 'https://www.mintscan.io/osmosis/txs/'+tx_id
    blockts = record['BLOCK_TIMESTAMP']
    address = record['ADDRESS']
    addr = address.split('1')[0]+'1...'+address[len(address) - 4:]
    action = record['ACTION']
    token_amount = record['TOKEN_AMOUNT']
    currency = record['PROJECT_NAME']
    amount_usd = record['AMOUNT_USD']
    to_currency = record['TO_PROJECT_NAME']
    to_token_amount = record['TO_TOKEN_AMOUNT']
    receiver = record['RECEIVER']
    if receiver is None: 
        rec = 0
    else:    
        rec = receiver.split('1')[0]+'1...'+receiver[len(receiver) -4:]
    time.sleep(20)
    if action == 'withdraw_rewards': 
      payload = f"{addr} withdrew {token_amount} {currency} (${amount_usd}) of staking rewards at {blockts} UTC.\n{link}"
    elif action == 'redelegate': 
      payload = f"{addr} redelegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC.\n{link}"
    elif action == 'pool_exited': 
      payload = f"{addr} withdrew {token_amount} {currency} (${amount_usd}) from a liquidity pool at {blockts} UTC.\n{link}"
    elif action == 'swap': 
      payload = f"{addr} swapped {token_amount} {currency} (${amount_usd}) for {to_token_amount} {to_currency} at {blockts} UTC.\n{link}"
    elif action == 'pool_joined': 
      payload = f"{addr} added {token_amount} {currency} (${amount_usd}) to a liquidity pool at {blockts} UTC.\n{link}"
    elif action == 'transfer': 
      payload = f"{addr} transferred {token_amount} {currency} (${amount_usd}) to {rec} at {blockts} UTC.\n{link}"
    elif action == 'delegate': 
      payload = f"{addr} delegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC.\n{link}"
    elif action == 'claim': 
      payload = f"{addr} claimed {token_amount} {currency} (${amount_usd}) at {blockts} UTC.\n{link}"
    else:
      payload = f"{addr} undelegated {token_amount} {currency} (${amount_usd}) at {blockts} UTC.\n{link}"

    api.update_status(payload)
    print('Done!')
