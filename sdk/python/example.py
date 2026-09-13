"""The cold-quickstart example: enqueue one job, run one worker, five lines."""

from cogs import Client, Worker

client = Client("http://localhost:8080", token="")


def send_email(job, ctx):
    print("sending to", job["payload"]["to"])


w = Worker(client, worker_id="py-1")
w.handle("emails", send_email)
w.run()
