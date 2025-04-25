from prefect import flow, task
from subflows.say_hello import say_hello


@flow(name="Hello Flow", log_prints=True)
def hello_flow():
    @task
    def tenant():
        print("-----------It's tenant1.-----------")

    @task
    def flow_end():
        print("-----------It's the end.-----------")

    tenant()
    say_hello("World~~~~")
    flow_end()


if __name__ == "__main__":
    hello_flow()
