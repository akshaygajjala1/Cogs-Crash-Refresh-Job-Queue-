// The cold-quickstart example: enqueue one job, run one worker, five lines.
import { Client, Worker } from "./index.js";

const client = new Client("http://localhost:8080", "");

const w = new Worker(client, "node-1");
w.handle("emails", async (job) => {
  console.log("sending to", job.payload.to);
});
await w.run();
