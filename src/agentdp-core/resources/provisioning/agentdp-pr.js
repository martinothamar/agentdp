#!/usr/bin/env node

const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");

const home = os.homedir();
const stateDir = process.env.AGENTDP_STATE_DIR || path.join(home, ".local/state/agentdp");
const registry = process.env.AGENTDP_PR_REGISTRY || path.join(stateDir, "pr-watch.json");

function usage() {
  console.error(`Usage:
  agentdp-pr create [gh pr create args...]
  agentdp-pr register [pr-url]`);
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd || process.cwd(),
    encoding: "utf8",
    stdio: options.capture ? ["inherit", "pipe", "pipe"] : "inherit",
  });
  if (result.status !== 0) {
    if (options.capture && result.stderr) {
      process.stderr.write(result.stderr);
    }
    process.exit(result.status || 1);
  }
  return options.capture ? result.stdout.trim() : "";
}

function repoRoot() {
  return run("git", ["rev-parse", "--show-toplevel"], { capture: true });
}

function currentBranch() {
  return run("git", ["branch", "--show-current"], { capture: true });
}

function readRegistry() {
  if (!fs.existsSync(registry)) {
    return { version: 1, prs: [] };
  }
  return JSON.parse(fs.readFileSync(registry, "utf8"));
}

function writeRegistryAtomic(contents) {
  fs.mkdirSync(path.dirname(registry), { recursive: true });
  const tmp = `${registry}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(contents, null, 2)}\n`);
  fs.renameSync(tmp, registry);
}

function ghPrView(target) {
  const args = [
    "pr",
    "view",
    target,
    "--json",
    "number,url,headRefName,baseRefName,title,state,updatedAt",
  ];
  return JSON.parse(run("gh", args, { capture: true }));
}

function register(target) {
  const repo = repoRoot();
  const branch = currentBranch();
  const pr = ghPrView(target || branch);
  const existing = readRegistry();
  const prs = (existing.prs || []).filter((entry) => entry.url !== pr.url);
  prs.push({
    ...pr,
    repo_path: repo,
    branch,
    registered_at: new Date().toISOString(),
  });
  writeRegistryAtomic({ version: 1, prs });
  console.log(pr.url);
}

function wantsHelp(args) {
  return args.includes("--help") || args.includes("-h");
}

function findPrUrl(output) {
  return output.match(/https:\/\/github\.com\/[^\s]+\/pull\/\d+/)?.[0] || "";
}

const [command, ...args] = process.argv.slice(2);
switch (command) {
  case "create":
    if (wantsHelp(args)) {
      run("gh", ["pr", "create", ...args]);
      break;
    }
    {
      const output = run("gh", ["pr", "create", ...args], { capture: true });
      if (output) {
        console.log(output);
      }
      register(findPrUrl(output));
    }
    break;
  case "register":
    if (wantsHelp(args)) {
      usage();
      break;
    }
    register(args[0] || "");
    break;
  case "help":
  case "--help":
  case "-h":
  case undefined:
    usage();
    break;
  default:
    usage();
    process.exit(2);
}
