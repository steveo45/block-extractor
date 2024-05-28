from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago

# Define the DAG
@dag(
        dag_id="test_parallel",
        default_args={'owner': 'airflow'}, start_date=days_ago(1), schedule_interval=None)
def parallel_task_group_example():

    @task
    def getblockheights():
        return [839848, 839849, 839850]
    @task
    def getblockhash(block_id: int):    
        return "000000000000000000011d55599ed27d7efca05f5849b755319c89eb2cffbc1f"

    @task
    def getblock(block_hash: str):
        return {"tx": [{"txid": "3331b7373b0ef76b753dac6f81e50422db3ddf3a5982fbd3799aace0974fd40e", "fee": 0.0001, "vsize": 100}]}

    @task
    def filterTransactionsFromBlock(getblockresult):
        return getblockresult['tx']

    @task
    def calculate_fee_rates(txid: str):
        return 0.0001

    @task
    def merge_results(values):
        # Flatten the list of lists
        flattened = [item for sublist in values for item in sublist]
        print(f"Merged Results: {flattened}")
        return flattened

    @task
    def calculate_median_fee_rate(fee_rates: list[float]) -> float:
        return sorted(fee_rates)[len(fee_rates) // 2]

    block_ids = getblockheights()
    # expand generates multiple parallel tasks
    calculated_values = calculate_fee_rates.expand(txid=filterTransactionsFromBlock.expand(getblockresult=getblock.expand(block_hash=getblockhash.expand(block_id=block_ids))))
    flattened_fee_rates = merge_results(calculated_values)
    print(flattened_fee_rates)
    median_fee_rate = calculate_median_fee_rate(flattened_fee_rates)
    print(median_fee_rate)


# Instantiate the DAG
dag = parallel_task_group_example()
        # @task
        # def txid_chunks_test(block):
        #     return [[5,4,3,2], [1,2,3,4], [5,6,7,8]]

        # @task
        # def get_txid_chunks(get_block_result, chunk_size) -> list[list[str]]:
        #     return [["3331b7373b0ef76b753dac6f81e50422db3ddf3a5982fbd3799aace0974fd40e"], ["3331b7373b0ef76b753dac6f81e50422db3ddf3a5982fbd3799aace0974fd40e"]]
        
        # print(f"Processing block {block_id}")
        # getblock(getblockhash(block_id))