import requests
import json
import pandas as pd

def oAuth_example():
    # Step 1: Get the OAuth token
    url = "https://oauth2.bitquery.io/oauth2/token"

    payload = 'grant_type=client_credentials&client_id=041eaf4d-6562-4950-b4ce-4200377ed006&client_secret=HYKTcfLX0mqE--mlmVdQut9IUo&scope=api'

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    response = requests.request("POST", url, headers=headers, data=payload)
    resp = json.loads(response.text)
    access_token = resp['access_token']

    # Step 2: Define the GraphQL subscription query
    subscription_query = """
    subscription MyQuery {
      EVM(network: eth) {
        Transactions(where: {Transaction: {}, TransactionStatus: {Success: true}}) {
          Transaction {
            From
            Cost
            CostInUSD
            GasPrice
            GasFeeCap
            Gas
            Value
            GasPriceInUSD
          }
        }
      }
    }
    """

    # Step 3: Use the token to send a request for the streaming data
    url_graphql = "https://streaming.bitquery.io/graphql"
    headers_graphql = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    # Send the GraphQL subscription query
    payload_graphql = json.dumps({"query": subscription_query})
    
    # Initiating the subscription
    response_data = requests.request("POST", url_graphql, headers=headers_graphql, data=payload_graphql)
    
    # Parse the streaming response (in a production case, you would handle the streaming part here)
    resp_data = json.loads(response_data.text)
    
    # Step 4: Convert response to DataFrame
    transaction_data = resp_data['data']['EVM']['Transactions']

    # Create a DataFrame from the transaction data
    df = pd.DataFrame([{
        'From': tx['Transaction']['From'],
        'Cost': tx['Transaction']['Cost'],
        'CostInUSD': tx['Transaction']['CostInUSD'],
        'GasPrice': tx['Transaction']['GasPrice'],
        'GasFeeCap': tx['Transaction']['GasFeeCap'],
        'Gas': tx['Transaction']['Gas'],
        'Value': tx['Transaction']['Value'],
        'GasPriceInUSD': tx['Transaction']['GasPriceInUSD']
    } for tx in transaction_data])

    # Convert DataFrame to CSV string
    csv_string = df.to_csv(index=False)

    print(csv_string)  # This will print the CSV format

# Call the function to get the streaming data and convert to CSV string
oAuth_example()
