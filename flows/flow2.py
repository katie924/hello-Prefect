from prefect import flow, task
from subflows.say_hello import say_hello


@flow(name="Another Flow")
def hello_flow():
    @task
    def tenant():
        print("It's tenant2.")

    tenant()
    say_hello("World!!")


if __name__ == "__main__":
    hello_flow()
