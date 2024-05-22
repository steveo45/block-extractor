"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
import json
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

block_height = 839848
rpc_url = "https://special-indulgent-darkness.btc.quiknode.pro/2386795a83ca78b965d15570b031b5b8c05b6a46/"
headers = {'content-type': 'application/json'}



@dag(
    dag_id="process_block",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def BlockExtractor():
    @task
    def get_block_hash():
        payload = json.dumps({
            "method": 'getblockhash',
            "params": [block_height],
            "jsonrpc": "2.0",
        })
        response = requests.post(rpc_url, headers=headers, data=payload)
        block_hash = response.json()['result']
        return block_hash

    @task
    def getblock(block_hash:str) -> dict:
        payload = json.dumps({
            "method": 'getblock',
            "params": [block_hash, 1],
            "jsonrpc": "2.0",
        })
        response = requests.post(rpc_url, headers=headers, data=payload)
        return response.json()['result']

    @task
    def get_txid_chunks(get_block_result, chunk_size) -> list[list[str]]:
        txids = get_block_result['tx']
        chunks = [txids[i:i + chunk_size] for i in range(0, len(txids), chunk_size)]
        return chunks

    @task
    def calculate_fee_rates(txids: list[str]) -> list[float]:
        rpc_url = "https://special-indulgent-darkness.btc.quiknode.pro/2386795a83ca78b965d15570b031b5b8c05b6a46/"
        headers = {'content-type': 'application/json'}
        fee_rates = []
        for txid in txids:
            payload = json.dumps({
                "method": 'getrawtransaction',
                "params": [txid, 2],
                "jsonrpc": "2.0",
            })
            response = requests.post(rpc_url, headers=headers, data=payload)
            tx = response.json()['result']

            if 'fee' in tx:
                fee_rate = tx['fee'] / tx['vsize']
                fee_rates.append(fee_rate)
            else:
                fee_rate = None

        return fee_rates

    @task
    def calculate_median_fee_rate(fee_rates: list[float]) -> float:
        return sorted(fee_rates)[len(fee_rates) // 2]
  
    block_hash = get_block_hash()
    block = getblock(block_hash)
    txid_chunks = get_txid_chunks(block['tx'], 50)
    # aggregated_feerates = []
    # for chunk in enumerate(txid_chunks):
    #     fee_rates = calculate_fee_rates(chunk)
    #     aggregated_feerates.extend(fee_rates)
    # median_fee_rate = calculate_median_fee_rate(aggregated_feerates)
    # print(f"Median fee rate for block {block_height} is {median_fee_rate}")

# instantiate the DAG
BlockExtractor()


