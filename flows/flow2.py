from prefect import flow, task
from subflows.say_hello import say_hello


@task
def tenant():
    print("It's tenant2.")


@flow(name="Another Flow")
def hello_flow():
    tenant()
    say_hello("World!!")


if __name__ == "__main__":
    hello_flow()
