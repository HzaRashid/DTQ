# dtq/cli.py
import os
import sys
import argparse
import importlib
import logging

def load_dtq_from_app_path(app_path: str):
    """
    app_path forms:
      - 'pkg.module' that exposes a 'dtq' singleton:     from pkg.module import dtq
      - 'pkg.module:attr' where attr is the DTQ object:   from pkg.module import attr as dtq
      - 'pkg.module' that exposes 'create_dtq()' factory: call to build an instance
      - 'pkg' (package): we'll create a default DTQ and autodiscover under this package
    """
    mod_name, _, attr = app_path.partition(":")
    mod = importlib.import_module(mod_name)

    if attr:
        dtq = getattr(mod, attr)
        return dtq

    # Try common conventions
    if hasattr(mod, "dtq"):
        return getattr(mod, "dtq")

    if hasattr(mod, "create_dtq"):
        return mod.create_dtq()

    # Fallback: create a DTQ and autodiscover in the given module/package
    from .app import DTQ  # your DTQ class
    dtq = DTQ(
        exchange=os.getenv("DTQ_EXCHANGE", "dtq"),
        default_queue=os.getenv("DTQ_DEFAULT_QUEUE", "default"),
        broker_url=os.getenv("RMQ_URL", "amqp://guest:guest@localhost:5672/%2F"),
        backend_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    )

    # If it's a package/module, try autodiscovery under it
    related = os.getenv("DTQ_RELATED_NAME", "tasks")
    try:
        # Ensure import ok; autodiscover will recurse if it's a package
        importlib.import_module(mod_name)
        dtq.autodiscover_tasks([mod_name], related_name=related)
    except Exception:
        pass

    return dtq


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dtq")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # worker subcommand
    w = sub.add_parser("worker", help="Start a DTQ worker")
    w.add_argument("-A", "--app", required=True, help="App path (e.g., yourproj.dtq_app[:dtq])")
    w.add_argument("-Q", "--queue", default=os.getenv("DTQ_QUEUE", "default"))
    w.add_argument("--prefetch", type=int, default=int(os.getenv("DTQ_PREFETCH", "1")))
    w.add_argument("--loglevel", default=os.getenv("DTQ_LOGLEVEL", "info"),
                   choices=["debug", "info", "warning", "error"])

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper()),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.cmd == "worker":
        dtq = load_dtq_from_app_path(args.app)
        # Importing the app module typically already imported task modules.
        # If your app uses envs DTQ_PACKAGES/DTQ_RELATED_NAME, it can autodiscover on import.

        # Start worker
        # from ..services.worker.runner import Worker  # your Worker class (uses the DTQ instance)
        from services.worker.runner import Worker  # your Worker class (uses the DTQ instance)
        Worker(dtq, queue=args.queue, prefetch=args.prefetch).start()


if __name__ == "__main__":
    main()
