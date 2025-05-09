from prefect import flow, task
from lib.utils import print_hello
from subflows.etl_metric import main


@flow()
def say_hello(name):
    @task
    def print_sub():
        print("-----------It's from a subflow.-----------")

    @task
    def say_hello_task(name):
        print_hello(name)

    @task
    def run_metric_data():
        tenant_id = 'customer'
        main(tenant_id)

    print_sub()
    say_hello_task(name)
    run_metric_data()
