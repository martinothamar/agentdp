'use strict';

const assert = require('node:assert/strict');
const Module = require('node:module');
const test = require('node:test');

test('reconciles healthy instances while retaining and retrying only failed instances', async () => {
  let remoteHosts = [
    { name: 'Personal host', address: 'wss://personal.example' },
    { name: 'AgentDP: agent/replica-0', address: 'wss://healthy-old.example' },
    { name: 'AgentDP: agent/replica-1', address: 'wss://failed-old.example' },
    { name: 'AgentDP: agent/replica-2', address: 'wss://stopped.example' },
  ];
  const vscode = {
    ConfigurationTarget: { Global: 1 },
    workspace: {
      getConfiguration(section) {
        assert.equal(section, 'chat');
        return {
          get: () => remoteHosts,
          update: async (_key, value) => { remoteHosts = value; },
        };
      },
    },
  };
  const originalLoad = Module._load;
  Module._load = function load(request, parent, isMain) {
    return request === 'vscode' ? vscode : originalLoad.call(this, request, parent, isMain);
  };
  const { DiscoveryController } = require('../extension');
  Module._load = originalLoad;

  const output = [];
  const controller = new DiscoveryController({ appendLine: line => output.push(line) });
  const originalFetch = global.fetch;
  global.fetch = async url => {
    if (url.pathname.endsWith('/1/status')) {
      throw new Error('status unavailable');
    }
    return {
      ok: true,
      json: async () => ({
        status: {
          phase: 'running',
          tailscaleServe: {
            routes: [{
              service: 'agent_host',
              status: 'applied',
              url: 'https://healthy-new.example',
            }],
          },
        },
      }),
    };
  };
  const instances = [
    { agent: 'agent', instance: 'replica-0', instance_id: 0, status: 'running', ready: true },
    { agent: 'agent', instance: 'replica-1', instance_id: 1, status: 'running', ready: true },
    { agent: 'agent', instance: 'replica-2', instance_id: 2, status: 'stopped', ready: false },
  ];

  try {
    await assert.rejects(
      controller.reconcile('https://control.example', instances, new AbortController().signal),
      /Could not refresh 1 AgentDP instance/,
    );
  } finally {
    global.fetch = originalFetch;
  }

  assert.deepEqual(remoteHosts, [
    { name: 'Personal host', address: 'wss://personal.example' },
    { name: 'AgentDP: agent/replica-0', address: 'wss://healthy-new.example' },
    { name: 'AgentDP: agent/replica-1', address: 'wss://failed-old.example' },
  ]);
  assert(output.some(line => line.includes('Could not refresh agent/replica-1')));
});
