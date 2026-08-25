"""The payments team's worker -- in Python -- for a flow whose topology was authored in Java.

It never re-declares the workflow. It binds ONE step, ``charge``, to the ``polyglot-order`` graph by
name; :meth:`Worker.start` reconciles that binding against the graph the Java author registered on
the server, discovers that ``charge`` polls the ``payments`` queue, and serves it. Run the Java side
first so the graph exists and instances are flowing::

    ./gradlew :example:runPolyglot           # terminal 1: server + author + Java worker + a submitted order
    python clients/python/examples/polyglot_worker.py   # terminal 2: this payments worker

The Java demo completes and prints the result as soon as this worker processes ``charge``.
"""
import os

from wiggle import WiggleClient, Worker


def charge(ctx: dict) -> dict:
    """Authorise the payment -- the one step the payments team owns."""
    print(f"   [py-payments] charging order {ctx['orderId']}")
    return {**ctx, "paymentRef": f"auth-{ctx['orderId']}"}


def main() -> None:
    url = os.environ.get("WIGGLE_URL", "localhost:8080")
    with WiggleClient(url) as client:
        # No blueprint, no topology -- just implement `charge` by (workflow, step) name.
        # await_registration_s rides out the race if this starts before the Java author registers.
        worker = Worker(client, "py-payments", await_registration_s=30).handle(
            "polyglot-order", "charge", charge)
        worker.start()   # reconciles: validates the step exists + is a task, learns the `payments` queue
        print("[py-payments] serving `charge` on the payments queue; Ctrl-C to stop")
        worker.run_forever()


if __name__ == "__main__":
    main()
