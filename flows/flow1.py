from prefect import flow, task
from subflows.say_hello import say_hello


@task
def tenant():
    print("It's tenant1.")


@flow(name="Hello Flow")
def hello_flow():
    tenant()
    say_hello("World~~~~")


if __name__ == "__main__":
    hello_flow()
