"""End-to-end example: define an order-fulfilment workflow, run a worker, submit orders.

    # start a server first, e.g. the Docker image or `./gradlew :dist:run`
    python examples/order.py
"""
import os

from wiggle import Retry, WiggleClient, Worker, Workflow


def build() -> "Workflow":
    return (
        Workflow("py-order")
        .step("validate", lambda o: {**o, "status": "VALIDATED"})
        .gate("in-stock", lambda o: o["quantity"] > 0)
        .step("charge", lambda o: {**o, "paymentRef": f"auth-{o['orderId']}"},
              queue="payments", retry=Retry.exponential(5, 0.1))
        .step("ship", lambda o: {**o, "trackingLabel": f"DHL-{o['orderId']}"})
        .effect("notify", lambda o: print(f"   [worker] {o['orderId']} -> {o['status']} "
                                          f"paid={o.get('paymentRef')} tracking={o.get('trackingLabel')}"))
        .build()
    )


def main() -> None:
    url = os.environ.get("WIGGLE_URL", "localhost:8080")
    wf = build()
    with WiggleClient(url) as client:
        client.register(wf)
        worker = Worker(client, "py-worker-1").register(wf).start()
        try:
            ids = [client.start(wf, {"orderId": f"A-{1000 + i}", "quantity": 1 + (i % 3)})
                   for i in range(5)]
            for iid in ids:
                view = client.await_completion(iid, timeout_s=30)
                print(f"  {iid}  {view.status}"
                      + (f"  ({view.termination_reason})" if view.termination_reason else "")
                      + (f"  ERROR: {view.error}" if view.error else ""))
        finally:
            worker.stop()


if __name__ == "__main__":
    main()
