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

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def block_extractor():
    @task(
        outlets=[Dataset("block_hash")]
    )
    def get_block_hash(**context) -> dict:
        rpc_url = "https://special-indulgent-darkness.btc.quiknode.pro/2386795a83ca78b965d15570b031b5b8c05b6a46/"
        headers = {'content-type': 'application/json'}
        block_height = 839848
        payload = json.dumps({
            "method": 'getblockhash',
            "params": [block_height],
            "jsonrpc": "2.0",
        })
        response = requests.post(rpc_url, headers=headers, data=payload)
        block_hash = response.json()['result']
        height_to_hash = {str(block_height): block_hash}
        return height_to_hash

    @task
    def print_block_hash(height_to_hash: dict) -> None:
        """
        This task creates a print statement with the block hash
        """
        print(f"Block hash for block height {list(height_to_hash.keys())[0]} is {list(height_to_hash.values())[0]}")

    @task
    def getblock(hash: str) -> list[str]:
        rpc_url = "https://special-indulgent-darkness.btc.quiknode.pro/2386795a83ca78b965d15570b031b5b8c05b6a46/"
        headers = {'content-type': 'application/json'}
        payload = json.dumps({
            "method": 'getblock',
            "params": [hash, 1],
            "jsonrpc": "2.0",
        })
        response = requests.post(rpc_url, headers=headers, data=payload)
        return response.json()['result']
    
    @task
    def get_txids_for_block(result) -> list[str]:
        return result['tx']

    @task
    def calculate_fee_rates(txids: list[str]) -> float:
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
    print_block_hash(block_hash)
    get_block_response = getblock(block_hash['839848'])

    txids = get_txids_for_block(get_block_response)
    fee_rates = calculate_fee_rates(txids)
    median_fee_rate = calculate_median_fee_rate(fee_rates)
    print(f"Fee rates for the first 100 transactions: {fee_rates}")
    print(f"Median fee rate for the first 20 transactions: {median_fee_rate}")
  

# instantiate the DAG
block_extractor()

# def example_astronauts():
#     # Define tasks
#     @task(
#         # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
#         outlets=[Dataset("current_astronauts")]
#     )  # Define that this task updates the `current_astronauts` Dataset
#     def get_astronauts(**context) -> list[dict]:
#         """
#         This task uses the requests library to retrieve a list of Astronauts
#         currently in space. The results are pushed to XCom with a specific key
#         so they can be used in a downstream pipeline. The task returns a list
#         of Astronauts to be used in the next task.
#         """
#         r = requests.get("http://api.open-notify.org/astros.json")
#         number_of_people_in_space = r.json()["number"]
#         list_of_people_in_space = r.json()["people"]

#         context["ti"].xcom_push(
#             key="number_of_people_in_space", value=number_of_people_in_space
#         )
#         return list_of_people_in_space

#     @task
#     def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
#         """
#         This task creates a print statement with the name of an
#         Astronaut in space and the craft they are flying on from
#         the API request results of the previous task, along with a
#         greeting which is hard-coded in this example.
#         """
#         craft = person_in_space["craft"]
#         name = person_in_space["name"]

#         print(f"{name} is currently in space flying on the {craft}! {greeting}")

#     # Use dynamic task mapping to run the print_astronaut_craft task for each
#     # Astronaut in space
#     print_astronaut_craft.partial(greeting="Hello! :)").expand(
#         person_in_space=get_astronauts()  # Define dependencies using TaskFlow API syntax
#     )


# # Instantiate the DAG
# example_astronauts()
