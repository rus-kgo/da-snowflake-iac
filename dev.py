import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from contextlib import contextmanager


# ---------------------------
# Connection State
# ---------------------------


class ConnectionState:
    def __init__(self, name):
        self.name = name
        self.state = "init"
        self.handle = str | None


# ---------------------------
# Base Connection Manager
# ---------------------------


class BaseConnectionManager:
    def __init__(self):
        self.thread_connections = {}
        self.lock = threading.RLock()

    def get_thread_id(self):
        return threading.current_thread().name

    def get_thread_connection(self):
        thread_id = self.get_thread_id()

        with self.lock:
            if thread_id not in self.thread_connections:
                print(f"[{thread_id}] creating ConnectionState")
                self.thread_connections[thread_id] = ConnectionState(thread_id)

            return self.thread_connections[thread_id]

    def open(self, connection: ConnectionState):
        print(f"[{connection.name}] OPEN connection")
        connection.state = "open"
        connection.handle = f"conn-{connection.name}"
        return connection

    def close(self, connection: ConnectionState):
        print(f"[{connection.name}] CLOSE connection")
        connection.state = "closed"
        connection.handle = None

    @contextmanager
    def get_connection(self):
        conn = self.get_thread_connection()

        if conn.state != "open":
            conn = self.open(conn)

        try:
            yield conn.handle
        except Exception:
            conn.state = "fail"
            raise

    def cleanup_all(self):
        with self.lock:
            for conn in self.thread_connections.values():
                if conn.state == "open":
                    self.close(conn)
            self.thread_connections.clear()


# ---------------------------
# Adapter
# ---------------------------


class MockAdapter:
    def __init__(self):
        self.connections = BaseConnectionManager()

    def execute(self, task_name: str):
        with self.connections.get_connection() as conn:
            thread = threading.current_thread().name
            print(f"[{thread}] USING {conn} START → {task_name}")
            time.sleep(1)  # simulate blocking I/O (SQL)
            print(f"[{thread}] USING {conn} END  → {task_name}")

    def cleanup(self):
        self.connections.cleanup_all()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.cleanup()


# ---------------------------
# dbt-style Scheduler
# ---------------------------


class Scheduler:
    def __init__(self, adapter, dependencies, max_threads=3):
        self.adapter = adapter
        self.dependencies = {k: set(v) for k, v in dependencies.items()}
        self.reverse_deps = defaultdict(set)
        self.max_threads = max_threads

        for node, deps in self.dependencies.items():
            for d in deps:
                self.reverse_deps[d].add(node)

    def run_task(self, task_name: str):
        self.adapter.execute(task_name)

    def run(self):
        completed = set()
        ready = {n for n, d in self.dependencies.items() if not d}
        futures = {}

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            while ready or futures:
                # submit all ready tasks
                while ready:
                    task = ready.pop()
                    future = executor.submit(self.run_task, task)
                    futures[future] = task

                # wait for one task to finish
                for future in as_completed(futures):
                    task = futures.pop(future)
                    future.result()  # propagate errors
                    completed.add(task)

                    # unlock downstream tasks
                    for child in self.reverse_deps[task]:
                        self.dependencies[child].remove(task)
                        if not self.dependencies[child]:
                            ready.add(child)

                    break  # important: return to scheduling loop


# ---------------------------
# Main
# ---------------------------

if __name__ == "__main__":
    # Dependency graph
    #
    # A     B
    #  \   /
    #    C
    #    |
    #    D
    #
    dependencies = {
        "A": set(),
        "B": set(),
        "C": {"A", "B"},
    }

    dependencies = {
        "database::my_db": {"role::admin_role", "role::user_role"},
        "schma::my_schema": set(),
        # "role::admin_role": {"database::my_db"},
    }

    adapter = MockAdapter()
    with adapter:
        scheduler = Scheduler(adapter, dependencies, max_threads=4)
        scheduler.run()
