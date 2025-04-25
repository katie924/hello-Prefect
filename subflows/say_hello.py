from prefect import flow, task
from lib.utils import print_hello


@flow(log_prints=True)
def say_hello(name):
    @task
    def print_sub():
        print("-----------It's from a subflow.-----------")

    @task
    def say_hello_task(name):
        print_hello(name)

    print_sub()
    say_hello_task(name)
