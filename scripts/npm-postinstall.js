#!/usr/bin/env node
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

if (process.env.KOSIS_MCP_SKIP_PYTHON_INSTALL === "1") {
  process.exit(0);
}

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const requirements = join(root, "requirements.txt");
if (!existsSync(requirements)) {
  process.exit(0);
}

const candidates = process.env.KOSIS_PYTHON
  ? [process.env.KOSIS_PYTHON]
  : process.platform === "win32"
    ? ["python", "py"]
    : ["python3", "python"];

let lastError = null;
for (const command of candidates) {
  const result = spawnSync(command, ["-m", "pip", "install", "-r", requirements], {
    stdio: "inherit",
    env: process.env,
  });
  if (result.status === 0) {
    process.exit(0);
  }
  lastError = result.error;
}

console.error("Python dependency installation failed.");
if (lastError) {
  console.error(lastError.message);
}
console.error("Install manually with: python -m pip install -r requirements.txt");
process.exit(0);
