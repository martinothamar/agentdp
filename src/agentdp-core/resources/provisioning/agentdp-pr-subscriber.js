#!/usr/bin/env node

const crypto = require("crypto");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");

const home = os.homedir();
const stateDir = process.env.AGENTDP_STATE_DIR || path.join(home, ".local/state/agentdp");
const registryPath = process.env.AGENTDP_PR_REGISTRY || path.join(stateDir, "pr-watch.json");
const paneFile = process.env.AGENTDP_CODEX_PANE_FILE || path.join(stateDir, "codex-pane-id");
const seenPath = path.join(stateDir, "pr-subscriber-seen.json");
const queueDir = path.join(stateDir, "pr-subscriber-queue");
const pollSeconds = Number(process.env.AGENTDP_PR_POLL_SECONDS || "60");
const idleSeconds = Number(process.env.AGENTDP_PR_IDLE_SECONDS || "20");
const maxPreviewLength = 220;
const trustedAuthorAssociations = new Set(["OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR"]);
let cachedGhLogin;
let lastPaneCapture = null;

fs.mkdirSync(stateDir, { recursive: true });
fs.mkdirSync(queueDir, { recursive: true });

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd || process.cwd(),
    encoding: "utf8",
    input: options.input,
    stdio: options.input === undefined ? ["ignore", "pipe", "pipe"] : ["pipe", "pipe", "pipe"],
  });
  if (result.status !== 0) {
    return null;
  }
  return result.stdout;
}

function readJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJsonAtomic(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(value, null, 2)}\n`);
  fs.renameSync(tmp, file);
}

function hash(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function registeredPrs() {
  const registry = readJson(registryPath, { prs: [] });
  return Array.isArray(registry.prs) ? registry.prs : [];
}

function currentGhLogin() {
  if (cachedGhLogin !== undefined) return cachedGhLogin;
  cachedGhLogin = (run("gh", ["api", "user", "--jq", ".login"]) || "").trim();
  return cachedGhLogin;
}

function authoredBySelf(item, selfLogin) {
  return Boolean(selfLogin && item.author?.login === selfLogin);
}

function trustedAuthor(item) {
  return trustedAuthorAssociations.has(item.authorAssociation || "");
}

function prView(url) {
  const stdout = run("gh", [
    "pr",
    "view",
    url,
    "--json",
    "number,title,state,url,headRefName,baseRefName,updatedAt,reviewDecision,statusCheckRollup,reviews,comments",
  ]);
  return stdout ? JSON.parse(stdout) : null;
}

function parsePrUrl(url) {
  const match = url.match(/^https:\/\/github\.com\/([^/]+)\/([^/]+)\/pull\/(\d+)/);
  if (!match) return null;
  return { owner: match[1], repo: match[2], number: match[3] };
}

function compactText(value) {
  if (!value) return "";
  const text = String(value)
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/<details[\s\S]*?<\/details>/gi, " [details omitted] ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return text.length > maxPreviewLength ? `${text.slice(0, maxPreviewLength - 1)}...` : text;
}

function checkName(check) {
  return check.name || check.context || check.workflowName || "unknown";
}

function checkState(check) {
  return {
    name: checkName(check),
    status: check.status || check.state || null,
    conclusion: check.conclusion || null,
    url: check.detailsUrl || check.targetUrl || null,
  };
}

function prPrefix(view) {
  const parsed = parsePrUrl(view.url);
  const number = view.number || parsed?.number || "?";
  return `pr=#${number} url=${view.url}`;
}

function quote(value) {
  return JSON.stringify(String(value));
}

function eventId(event) {
  return hash(JSON.stringify(event.identity));
}

function failedCheckEvents(view) {
  return (view.statusCheckRollup || [])
    .map(checkState)
    .filter((check) => ["FAILURE", "ERROR", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED"].includes(check.conclusion))
    .map((check) => {
      const status = check.conclusion || check.status || "unknown";
      return {
        identity: {
          type: "check_failed",
          url: view.url,
          name: check.name,
          status,
        },
        line: `${prPrefix(view)} event=check_failed status=${status} name=${quote(check.name)}${check.url ? ` details=${check.url}` : ""}`,
      };
    });
}

function reviewEvents(view, selfLogin) {
  return (view.reviews || [])
    .filter((review) => !authoredBySelf(review, selfLogin))
    .map((review) => {
      const trusted = trustedAuthor(review);
      const author = review.author?.login || "unknown";
      const state = review.state || "UNKNOWN";
      const submittedAt = review.submittedAt || "unknown";
      const preview = trusted ? compactText(review.body || "") : "";
      return {
        identity: {
          type: "review",
          url: view.url,
          author,
          state,
          submittedAt,
          body: preview,
        },
        line: `${prPrefix(view)} event=review state=${state} author=${author} at=${submittedAt}${preview ? ` body=${quote(preview)}` : ""}`,
      };
    });
}

function commentEvents(view, selfLogin) {
  return (view.comments || [])
    .filter((comment) => !authoredBySelf(comment, selfLogin))
    .map((comment) => {
      const trusted = trustedAuthor(comment);
      const author = comment.author?.login || "unknown";
      const updatedAt = comment.updatedAt || comment.createdAt || "unknown";
      const url = comment.url || view.url;
      const preview = trusted ? compactText(comment.body || "") : "";
      return {
        identity: {
          type: "comment",
          url,
          author,
          updatedAt,
          body: preview,
        },
        line: `${prPrefix(view)} event=comment author=${author} at=${updatedAt} comment=${url}${preview ? ` body=${quote(preview)}` : ""}`,
      };
    });
}

function prEvents(view, selfLogin) {
  return [
    ...failedCheckEvents(view),
    ...reviewEvents(view, selfLogin),
    ...commentEvents(view, selfLogin),
  ].map((event) => ({ id: eventId(event), line: event.line }));
}

function renderPrompt(events) {
  return `<pr_events>
${events.map((event) => event.line).join("\n")}
</pr_events>
`;
}

function paneId() {
  try {
    return fs.readFileSync(paneFile, "utf8").trim();
  } catch {
    return "";
  }
}

function paneExists(id) {
  const panes = run("tmux", ["list-panes", "-a", "-F", "#{pane_id}"]);
  return panes ? panes.split(/\r?\n/).includes(id) : false;
}

function capturePane(id) {
  return run("tmux", ["capture-pane", "-p", "-t", id, "-S", "-80"]) || "";
}

function codexLooksIdle(id) {
  if (idleSeconds <= 0) return true;
  const capture = capturePane(id);
  if (!capture) return false;

  const now = Date.now();
  const captureHash = hash(capture);
  if (!lastPaneCapture || lastPaneCapture.id !== id || lastPaneCapture.hash !== captureHash) {
    lastPaneCapture = { id, hash: captureHash, since: now };
    return false;
  }

  return now - lastPaneCapture.since >= idleSeconds * 1000;
}

function injectPrompt(id, events) {
  const promptFile = path.join(stateDir, `pr-prompt.${process.pid}.txt`);
  fs.writeFileSync(promptFile, renderPrompt(events));
  run("tmux", ["load-buffer", "-b", "agentdp-pr", promptFile]);
  run("tmux", ["paste-buffer", "-b", "agentdp-pr", "-t", id, "-p", "-r"]);
  run("tmux", ["send-keys", "-t", id, "Enter"]);
  fs.rmSync(promptFile, { force: true });
}

function queueEvents(key, url, events) {
  const file = path.join(queueDir, `${key}.json`);
  const queued = readJson(file, { url, events: [] });
  const byId = new Map((queued.events || []).map((event) => [event.id, event]));
  for (const event of events) {
    byId.set(event.id, event);
  }
  writeJsonAtomic(file, {
    url,
    events: Array.from(byId.values()),
    updated_at: new Date().toISOString(),
  });
}

function queuedEventFiles() {
  try {
    return fs.readdirSync(queueDir)
      .filter((name) => name.endsWith(".json"))
      .map((name) => path.join(queueDir, name));
  } catch {
    return [];
  }
}

function flushQueuedEvents() {
  const files = queuedEventFiles();
  const events = files.flatMap((file) => readJson(file, { events: [] }).events || []);
  if (events.length === 0) return;

  const targetPane = paneId();
  if (!targetPane || !paneExists(targetPane) || !codexLooksIdle(targetPane)) return;

  injectPrompt(targetPane, events);
  for (const file of files) {
    fs.rmSync(file, { force: true });
  }
}

function handlePr(entry) {
  if (!entry.url) return;
  const view = prView(entry.url);
  if (!view) return;

  const selfLogin = currentGhLogin();
  const key = hash(entry.url);
  const events = prEvents(view, selfLogin);
  const seen = readJson(seenPath, { events: {} });
  const newEvents = events.filter((event) => !seen.events?.[event.id]);
  if (newEvents.length === 0) return;

  queueEvents(key, entry.url, newEvents);
  seen.events = { ...(seen.events || {}) };
  for (const event of newEvents) {
    seen.events[event.id] = true;
  }
  writeJsonAtomic(seenPath, seen);
}

async function main() {
  while (true) {
    for (const pr of registeredPrs()) {
      handlePr(pr);
    }
    flushQueuedEvents();
    await new Promise((resolve) => setTimeout(resolve, pollSeconds * 1000));
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
