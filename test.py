import json
import requests

def get_block_hash():
    rpc_url = "https://special-indulgent-darkness.btc.quiknode.pro/2386795a83ca78b965d15570b031b5b8c05b6a46/"
    headers = {'content-type': 'application/json'}
    block_height = 830000
    payload = json.dumps({
        "method": 'getblockhash',
        "params": [block_height],
        "jsonrpc": "2.0",
    })
    response = requests.post(rpc_url, headers=headers, data=payload)
    block_hash = response.json()['result']
    height_to_hash = {block_height: block_hash}
    return height_to_hash

def print_block_hash(height_to_hash: dict) -> None:
    """
    This task creates a print statement with the block hash
    """
    print(f"Block hash for block height {list(height_to_hash.keys())[0]} is {list(height_to_hash.values())[0]}")

# print_block_hash(get_block_hash())
  # Define dependencies using TaskFlow API syntax
def getblock(hash: str) -> dict:
    rpc_url = "https://special-indulgent-darkness.btc.quiknode.pro/2386795a83ca78b965d15570b031b5b8c05b6a46/"
    headers = {'content-type': 'application/json'}
    payload = json.dumps({
        "method": 'getblock',
        "params": [hash, 1],
        "jsonrpc": "2.0",
    })
    response = requests.post(rpc_url, headers=headers, data=payload)
    block = response.json()['result']['tx']
    return block

def calculate_fee_rates(txid) -> float:
    rpc_url = "https://special-indulgent-darkness.btc.quiknode.pro/2386795a83ca78b965d15570b031b5b8c05b6a46/"
    headers = {'content-type': 'application/json'}
    fee_rates = []
    txids = [txid]
    for txid in txids:
        payload = json.dumps({
            "method": 'getrawtransaction',
            "params": [txid, 2],
            "jsonrpc": "2.0",
        })
        response = requests.post(rpc_url, headers=headers, data=payload)
        tx = response.json()['result']
        print(tx)
        # fee_rate = tx['fee'] / tx['vsize']
        # fee_rates.append(fee_rate)
    return fee_rates
    
print(calculate_fee_rates('3331b7373b0ef76b753dac6f81e50422db3ddf3a5982fbd3799aace0974fd40e'))
    # print(getblock('000000000000000000011d55599ed27d7efca05f5849b755319c89eb2cffbc1f')[0])