from prefect import flow, task
from lib.utils import print_hello


@task
def say_hello_task(name):
    print_hello(name)


@flow(name="Say Hello Subflow")
def say_hello(name):
    say_hello_task(name)
