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
  agentdp-pr register [pr-url-or-number]
  agentdp-pr unregister [pr-url-or-number]
  agentdp-pr list`);
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

function prMatches(entry, target, pr) {
  if (pr) {
    return entry.url === pr.url;
  }
  return entry.url === target || String(entry.number) === target;
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

function unregister(target) {
  const pr = target ? null : ghPrView(currentBranch());
  const removeTarget = target || pr.url;
  const existing = readRegistry();
  const before = existing.prs || [];
  const prs = before.filter((entry) => !prMatches(entry, removeTarget, pr));
  writeRegistryAtomic({ version: 1, prs });
  if (prs.length === before.length) {
    console.error(`not registered: ${removeTarget}`);
    process.exit(1);
  }
  console.log(removeTarget);
}

function list() {
  for (const pr of readRegistry().prs || []) {
    const branch = pr.branch ? ` ${pr.branch}` : "";
    console.log(`#${pr.number} ${pr.url}${branch}`);
  }
}

function wantsHelp(args) {
  return args.includes("--help") || args.includes("-h");
}

const [command, ...args] = process.argv.slice(2);
switch (command) {
  case "register":
    if (wantsHelp(args)) {
      usage();
      break;
    }
    register(args[0] || "");
    break;
  case "unregister":
    if (wantsHelp(args)) {
      usage();
      break;
    }
    unregister(args[0] || "");
    break;
  case "list":
    if (wantsHelp(args)) {
      usage();
      break;
    }
    list();
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
