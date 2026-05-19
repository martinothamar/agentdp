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
const maxListedItems = 6;
const maxPreviewLength = 220;
const trustedAuthorAssociations = new Set(["OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR"]);
let cachedGhLogin;

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

function summarizedChecks(checks) {
  const normalized = checks.map(checkState);
  const failed = normalized.filter((check) => ["FAILURE", "ERROR", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED"].includes(check.conclusion));
  const pending = normalized.filter((check) =>
    check.conclusion === null && !["SUCCESS", "SKIPPED", "NEUTRAL"].includes(check.status),
  );
  const passed = normalized.filter((check) =>
    ["SUCCESS", "SKIPPED", "NEUTRAL"].includes(check.conclusion) ||
    check.status === "SUCCESS",
  );
  return {
    total: normalized.length,
    passed: passed.length,
    failed: failed.slice(0, maxListedItems),
    pending: pending.slice(0, maxListedItems),
  };
}

function summarizedReviews(reviews, selfLogin) {
  const normalized = reviews
    .filter((review) => !authoredBySelf(review, selfLogin))
    .map((review) => {
      const trusted = trustedAuthor(review);
      return {
        author: review.author?.login || "unknown",
        trusted,
        state: review.state || "UNKNOWN",
        submittedAt: review.submittedAt || null,
        preview: trusted ? compactText(review.body || "") : "",
      };
    });
  return {
    total: normalized.length,
    changesRequested: normalized.filter((review) => review.state === "CHANGES_REQUESTED").length,
    approved: normalized.filter((review) => review.state === "APPROVED").length,
    latest: normalized.slice(-maxListedItems),
  };
}

function summarizedComments(comments, selfLogin) {
  const normalized = comments
    .filter((comment) => !authoredBySelf(comment, selfLogin))
    .map((comment) => {
      const trusted = trustedAuthor(comment);
      return {
        author: comment.author?.login || "unknown",
        trusted,
        updatedAt: comment.updatedAt || comment.createdAt || null,
        url: comment.url || null,
        preview: trusted ? compactText(comment.body || "") : "",
      };
    });
  return {
    total: normalized.length,
    latest: normalized.slice(-3),
  };
}

function prSummary(view, selfLogin) {
  const checks = summarizedChecks(view.statusCheckRollup || []);
  const reviews = summarizedReviews(view.reviews || [], selfLogin);
  const comments = summarizedComments(view.comments || [], selfLogin);
  const parsed = parsePrUrl(view.url);
  return {
    pr: {
      number: view.number || parsed?.number || null,
      title: view.title || "",
      state: view.state || "",
      url: view.url,
      head: view.headRefName || "",
      base: view.baseRefName || "",
      updatedAt: view.updatedAt,
      reviewDecision: view.reviewDecision || "UNKNOWN",
    },
    checks,
    reviews,
    comments,
  };
}

function prFingerprint(view, selfLogin) {
  return {
    url: view.url,
    updatedAt: view.updatedAt,
    reviewDecision: view.reviewDecision,
    checks: (view.statusCheckRollup || []).map(checkState),
    reviews: (view.reviews || [])
      .filter((review) => !authoredBySelf(review, selfLogin))
      .map((review) => ({
        author: review.author?.login || "unknown",
        authorAssociation: review.authorAssociation || "",
        state: review.state,
        submittedAt: review.submittedAt,
        body: trustedAuthor(review) ? review.body || "" : "",
      })),
    comments: (view.comments || [])
      .filter((comment) => !authoredBySelf(comment, selfLogin))
      .map((comment) => ({
        author: comment.author?.login || "unknown",
        authorAssociation: comment.authorAssociation || "",
        updatedAt: comment.updatedAt || comment.createdAt || null,
        url: comment.url || null,
        body: trustedAuthor(comment) ? comment.body || "" : "",
      })),
  };
}

function bulletList(items, formatter, emptyText) {
  if (!items.length) return `- ${emptyText}`;
  return items.map((item) => `- ${formatter(item)}`).join("\n");
}

function checkLine(check) {
  const status = check.conclusion || check.status || "unknown";
  return `${check.name} (${status})${check.url ? ` ${check.url}` : ""}`;
}

function pendingCheckLine(check) {
  return `${check.name} (${check.status || "pending"})${check.url ? ` ${check.url}` : ""}`;
}

function reviewLine(review) {
  const submittedAt = review.submittedAt ? ` at ${review.submittedAt}` : "";
  const preview = review.trusted && review.preview
    ? ` - ${review.preview}`
    : review.trusted
      ? ""
      : " - body omitted";
  return `${review.author}: ${review.state}${submittedAt}${preview}`;
}

function commentLine(comment) {
  const updatedAt = comment.updatedAt ? ` at ${comment.updatedAt}` : "";
  const url = comment.url ? ` ${comment.url}` : "";
  const preview = comment.trusted && comment.preview
    ? ` - ${comment.preview}`
    : comment.trusted
      ? ""
      : " - body omitted";
  return `${comment.author}${updatedAt}${url}${preview}`;
}

function renderPrompt(summary) {
  const pr = summary.pr;
  const checks = summary.checks;
  const reviews = summary.reviews;
  const comments = summary.comments;
  const failedCount = checks.failed.length;
  const pendingCount = checks.pending.length;
  const promptReason = failedCount > 0
    ? "check failure"
    : reviews.changesRequested > 0
      ? "changes requested"
      : comments.latest.length > 0
        ? "new or updated PR comment"
        : pendingCount > 0
          ? "check status update"
          : "PR status update";

  return `<github_pr_summary_update>
Reason: ${promptReason}
PR: #${pr.number || "?"} ${pr.title}
URL: ${pr.url}
State: ${pr.state || "unknown"} | Review: ${pr.reviewDecision} | Branch: ${pr.head || "?"} -> ${pr.base || "?"}
Updated: ${pr.updatedAt || "unknown"}

Checks: ${checks.passed}/${checks.total} passed, ${checks.failed.length} failed, ${checks.pending.length} pending
Failed checks:
${bulletList(checks.failed, checkLine, "none")}
Pending checks:
${bulletList(checks.pending, pendingCheckLine, "none")}

Reviews: ${reviews.approved} approved, ${reviews.changesRequested} changes requested, ${reviews.total} total
Recent reviews:
${bulletList(reviews.latest, reviewLine, "none")}

Top-level comments: ${comments.total} total
Recent comments:
${bulletList(comments.latest, commentLine, "none")}
</github_pr_summary_update>
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

function injectPrompt(id, url, summary) {
  const promptFile = path.join(stateDir, `pr-prompt.${process.pid}.txt`);
  fs.writeFileSync(promptFile, renderPrompt(summary));
  run("tmux", ["load-buffer", "-b", "agentdp-pr", promptFile]);
  run("tmux", ["paste-buffer", "-b", "agentdp-pr", "-t", id, "-p", "-r"]);
  run("tmux", ["send-keys", "-t", id, "Enter"]);
  fs.rmSync(promptFile, { force: true });
}

function queuePrompt(key, url, summary) {
  writeJsonAtomic(path.join(queueDir, `${key}.json`), { url, summary });
}

function clearQueuedPrompt(key) {
  fs.rmSync(path.join(queueDir, `${key}.json`), { force: true });
}

function handlePr(entry) {
  if (!entry.url) return;
  const view = prView(entry.url);
  if (!view) return;

  const selfLogin = currentGhLogin();
  const summary = prSummary(view, selfLogin);
  const key = hash(entry.url);
  const fingerprint = hash(JSON.stringify(prFingerprint(view, selfLogin)));
  const seen = readJson(seenPath, { events: {} });
  if (seen.events?.[key] === fingerprint) return;

  const targetPane = paneId();
  if (targetPane && paneExists(targetPane)) {
    injectPrompt(targetPane, entry.url, summary);
    clearQueuedPrompt(key);
    seen.events = { ...(seen.events || {}), [key]: fingerprint };
    writeJsonAtomic(seenPath, seen);
  } else {
    queuePrompt(key, entry.url, summary);
  }
}

async function main() {
  while (true) {
    for (const pr of registeredPrs()) {
      handlePr(pr);
    }
    await new Promise((resolve) => setTimeout(resolve, pollSeconds * 1000));
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
