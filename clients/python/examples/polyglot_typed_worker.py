"""The payments worker (Python) for the *typed* polyglot flow.

The Java side models the context as a `Purchase` record; on the wire that's the same JSON object, so
here it's just a dict. This worker binds `charge` on `typed-order` by name — no workflow definition,
no shared type — and Java decodes the result back into its record. Run the Java demo first::

    ./gradlew :example:runTypedPolyglot                       # terminal 1
    python clients/python/examples/polyglot_typed_worker.py   # terminal 2
"""
import os

from wiggle import WiggleClient, Worker


def charge(purchase: dict) -> dict:
    """The one step the payments team owns — a dict in, a dict out."""
    print(f"   [py-payments] charging order {purchase['orderId']} (qty {purchase['quantity']})")
    return {**purchase, "paymentRef": f"auth-{purchase['orderId']}"}


def main() -> None:
    url = os.environ.get("WIGGLE_URL", "localhost:8080")
    with WiggleClient(url) as client:
        worker = Worker(client, "py-typed-payments", await_registration_s=30).handle(
            "typed-order", "charge", charge)
        worker.start()   # reconciles against the Java-authored typed-order graph
        print("[py-payments] serving `charge` on the payments queue; Ctrl-C to stop")
        worker.run_forever()


if __name__ == "__main__":
    main()
