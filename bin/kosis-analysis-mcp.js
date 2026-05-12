#!/usr/bin/env node
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const serverPath = join(root, "kosis_mcp_server.py");
const candidates = process.env.KOSIS_PYTHON
  ? [process.env.KOSIS_PYTHON]
  : process.platform === "win32"
    ? ["python", "py"]
    : ["python3", "python"];

function start(index = 0) {
  const command = candidates[index];
  if (!command) {
    console.error("No Python executable found. Set KOSIS_PYTHON to the Python path.");
    process.exit(1);
  }

  if (!process.env.KOSIS_API_KEY) {
    console.error("Warning: KOSIS_API_KEY is not set. Tools will require api_key arguments or fail on KOSIS calls.");
  }

  const child = spawn(command, [serverPath], {
    stdio: "inherit",
    env: process.env,
  });

  child.on("error", (error) => {
    if (error.code === "ENOENT") {
      start(index + 1);
      return;
    }
    console.error(`Failed to start ${command}:`, error);
    process.exit(1);
  });

  child.on("exit", (code, signal) => {
    if (signal) {
      process.kill(process.pid, signal);
      return;
    }
    process.exit(code ?? 0);
  });
}

start();
