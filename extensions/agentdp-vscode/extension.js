'use strict';

const vscode = require('vscode');
const {
  AGENTDP_HOST_PREFIX,
  discoveredHost,
  instanceKey,
  instanceStatusPath,
  mergeRemoteHosts,
} = require('./lib/discovery');

const EXTENSION_ID = 'martinothamar.agentdp';

class DiscoveryController {
  constructor(output) {
    this.output = output;
    this.abortController = undefined;
    this.loop = undefined;
    this.generation = 0;
  }

  async restart() {
    const generation = ++this.generation;
    await this.stopCurrentLoop();
    if (generation !== this.generation) {
      return;
    }
    const serverUrl = configuredServerUrl();
    if (!serverUrl) {
      await this.clearManagedHosts();
      if (generation !== this.generation) {
        return;
      }
      return;
    }
    this.abortController = new AbortController();
    this.loop = this.run(serverUrl, this.abortController.signal);
  }

  async stop() {
    ++this.generation;
    await this.stopCurrentLoop();
  }

  async stopCurrentLoop() {
    const loop = this.loop;
    this.abortController?.abort();
    if (loop) {
      await loop;
    }
    if (this.loop === loop) {
      this.abortController = undefined;
      this.loop = undefined;
    }
  }

  async clearManagedHosts() {
    const chat = vscode.workspace.getConfiguration('chat');
    const current = chat.get('remoteAgentHosts', []);
    const next = mergeRemoteHosts(current, []);
    if (JSON.stringify(current) !== JSON.stringify(next)) {
      await chat.update('remoteAgentHosts', next, vscode.ConfigurationTarget.Global);
    }
  }

  async refreshFrom(serverUrl, signal) {
    const response = await request(serverUrl, '/api/instances', signal);
    const result = await response.json();
    await this.reconcile(serverUrl, result.instances ?? [], signal);
  }

  async run(serverUrl, signal) {
    let delay = 1000;
    while (!signal.aborted) {
      try {
        await this.consumeEvents(serverUrl, signal);
        delay = 1000;
      } catch (error) {
        if (signal.aborted) {
          return;
        }
        this.output.appendLine(`Discovery connection failed: ${errorMessage(error)}; retrying in ${delay}ms`);
        await abortableDelay(delay, signal);
        delay = Math.min(delay * 2, 30_000);
      }
    }
  }

  async consumeEvents(serverUrl, signal) {
    await this.refreshFrom(serverUrl, signal);
    const response = await request(serverUrl, '/api/events', signal, 'text/event-stream');
    if (!response.body) {
      throw new Error('AgentDP event stream returned no body');
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (!signal.aborted) {
      const { done, value } = await reader.read();
      if (done) {
        throw new Error('AgentDP event stream closed');
      }
      buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
      let boundary;
      while ((boundary = buffer.indexOf('\n\n')) >= 0) {
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const event = parseEvent(block);
        if (event.name === 'instances') {
          const result = JSON.parse(event.data);
          await this.reconcile(serverUrl, result.instances ?? [], signal);
        }
      }
    }
  }

  async reconcile(serverUrl, instances, signal) {
    const chat = vscode.workspace.getConfiguration('chat');
    const current = chat.get('remoteAgentHosts', []);
    const previousByName = new Map(current
      .filter(host => host.name?.startsWith(AGENTDP_HOST_PREFIX))
      .map(host => [host.name, host]));
    const ready = instances.filter(instance => instance.status === 'running' && instance.ready === true);
    const next = [];

    const outcomes = await Promise.all(ready.map(async instance => {
      const key = instanceKey(instance);
      try {
        const path = instanceStatusPath(instance);
        const response = await request(serverUrl, path, signal);
        return { key, host: discoveredHost(instance, await response.json()) };
      } catch (error) {
        return { key, error };
      }
    }));

    if (signal?.aborted) {
      return;
    }
    const failures = [];
    for (const outcome of outcomes) {
      if ('error' in outcome) {
        failures.push(outcome);
        const existing = previousByName.get(`${AGENTDP_HOST_PREFIX}${outcome.key}`);
        if (existing) {
          next.push({ key: outcome.key, name: existing.name, address: existing.address });
        }
        this.output.appendLine(`Could not refresh ${outcome.key}: ${errorMessage(outcome.error)}`);
      } else if (outcome.host) {
        next.push(outcome.host);
      }
    }
    next.sort((left, right) => left.key.localeCompare(right.key));
    const latest = chat.get('remoteAgentHosts', []);
    const merged = mergeRemoteHosts(latest, next);
    if (JSON.stringify(latest) !== JSON.stringify(merged)) {
      await chat.update('remoteAgentHosts', merged, vscode.ConfigurationTarget.Global);
    }
    this.output.appendLine(`Discovered ${next.length} ready AgentDP Agent Host instance(s).`);
    if (failures.length > 0) {
      throw new Error(`Could not refresh ${failures.length} AgentDP instance(s)`);
    }
  }
}

async function request(serverUrl, path, signal, accept = 'application/json') {
  const headers = { Accept: accept };
  const response = await fetch(new URL(path, `${serverUrl}/`), { headers, signal });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText} from ${path}`);
  }
  return response;
}

function parseEvent(block) {
  let name = 'message';
  const data = [];
  for (const line of block.split('\n')) {
    if (line.startsWith('event:')) {
      name = line.slice('event:'.length).trim();
    } else if (line.startsWith('data:')) {
      data.push(line.slice('data:'.length).trimStart());
    }
  }
  return { name, data: data.join('\n') };
}

function configuredServerUrl() {
  const value = vscode.workspace.getConfiguration('agentdp').get('serverUrl', '').trim();
  return value.replace(/\/$/, '');
}

async function enableAgentsWindowSupport(canManageRemoteAgentHosts) {
  const extensions = vscode.workspace.getConfiguration('extensions');
  const supported = extensions.get('supportAgentsWindow', {});
  if (supported[EXTENSION_ID] !== true) {
    await extensions.update('supportAgentsWindow', { ...supported, [EXTENSION_ID]: true }, vscode.ConfigurationTarget.Global);
  }
  if (canManageRemoteAgentHosts) {
    const chat = vscode.workspace.getConfiguration('chat');
    await chat.update('remoteAgentHostsEnabled', true, vscode.ConfigurationTarget.Global);
    await chat.update('remoteAgentHostsAutoConnect', true, vscode.ConfigurationTarget.Global);
  }
}

async function addServer(controller, canManageRemoteAgentHosts) {
  const current = configuredServerUrl();
  const serverUrl = await vscode.window.showInputBox({
    title: 'Add AgentDP server',
    prompt: 'Enter the AgentDP control-plane Tailscale URL',
    value: current,
    placeHolder: 'https://desktop-cachy.example.ts.net',
    validateInput: value => {
      try {
        const url = new URL(value);
        return url.protocol === 'https:' || url.protocol === 'http:' ? undefined : 'Use an HTTP or HTTPS URL.';
      } catch {
        return 'Enter a valid URL.';
      }
    },
  });
  if (serverUrl === undefined) {
    return;
  }
  const normalized = serverUrl.trim().replace(/\/$/, '');
  await vscode.workspace.getConfiguration('agentdp').update('serverUrl', normalized, vscode.ConfigurationTarget.Global);
  await enableAgentsWindowSupport(canManageRemoteAgentHosts);
  if (canManageRemoteAgentHosts) {
    await controller.restart();
    void vscode.window.showInformationMessage(`AgentDP server added: ${normalized}`);
  } else {
    void vscode.window.showInformationMessage(`AgentDP server added: ${normalized}. Open a new Agents Window to start discovery.`);
  }
}

async function removeServer(controller, canManageRemoteAgentHosts) {
  if (!canManageRemoteAgentHosts) {
    void vscode.window.showInformationMessage('Remove the AgentDP server from the Agents Window so its managed hosts can be removed safely.');
    return;
  }
  await controller.stop();
  await vscode.workspace.getConfiguration('agentdp').update('serverUrl', '', vscode.ConfigurationTarget.Global);
  const chat = vscode.workspace.getConfiguration('chat');
  const current = chat.get('remoteAgentHosts', []);
  await chat.update('remoteAgentHosts', mergeRemoteHosts(current, []), vscode.ConfigurationTarget.Global);
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function abortableDelay(milliseconds, signal) {
  return new Promise(resolve => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timeout = setTimeout(resolve, milliseconds);
    signal.addEventListener('abort', () => {
      clearTimeout(timeout);
      resolve();
    }, { once: true });
  });
}

function activate(context) {
  const output = vscode.window.createOutputChannel('AgentDP');
  const controller = new DiscoveryController(output);
  const canManageRemoteAgentHosts = vscode.workspace.getConfiguration('chat').inspect('remoteAgentHosts') !== undefined;
  context.subscriptions.push(output, { dispose: () => { void controller.stop(); } });
  context.subscriptions.push(vscode.commands.registerCommand('agentdp.addServer', () => addServer(controller, canManageRemoteAgentHosts)));
  context.subscriptions.push(vscode.commands.registerCommand('agentdp.removeServer', () => removeServer(controller, canManageRemoteAgentHosts)));
  context.subscriptions.push(vscode.commands.registerCommand('agentdp.refreshAgentHosts', async () => {
    if (!canManageRemoteAgentHosts) {
      void vscode.window.showInformationMessage('AgentDP host discovery runs in the Agents Window.');
      return;
    }
    try {
      await controller.restart();
      void vscode.window.showInformationMessage('AgentDP host discovery restarted.');
    } catch (error) {
      void vscode.window.showErrorMessage(`AgentDP refresh failed: ${errorMessage(error)}`);
    }
  }));
  if (canManageRemoteAgentHosts) {
    context.subscriptions.push(vscode.workspace.onDidChangeConfiguration(event => {
      if (event.affectsConfiguration('agentdp.serverUrl')) {
        void controller.restart();
      }
    }));
    void controller.restart();
  }
}

function deactivate() {}

module.exports = { activate, deactivate, DiscoveryController };
