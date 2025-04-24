from prefect import serve

from flows.flow1 import hello_flow
from flows.flow2 import hello_flow as another_flow


if __name__ == "__main__":
    serve(
        hello_flow.to_deployment(name="Hello deploy"),
        another_flow.to_deployment(name="Another deploy")
    )
