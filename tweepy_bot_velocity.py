import requests
import tweepy
import time

## Twitter API Keys
consumer_key = "consumer_key"
consumer_secret = "consumer_secret"
access_token = "access_token"
access_token_secret = "access_token_secret"

## Twitter oAuth2 to gain API access
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

query_url = 'https://node-api.flipsidecrypto.com/api/v2/queries/9bf1a52f-c874-4297-9edc-08880d6bd154/data/latest'
query_result_set = requests.get(query_url)
data = query_result_set.json()

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
