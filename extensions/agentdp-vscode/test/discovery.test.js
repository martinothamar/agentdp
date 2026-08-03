'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { discoveredHost, instanceStatusPath, mergeRemoteHosts, toWebSocketUrl } = require('../lib/discovery');

test('addresses instance status by numeric API ID rather than display name', () => {
  const instance = {
    agent: 'altinn-studio',
    instance: 'replica-0',
    instance_id: 0,
  };

  assert.equal(instanceStatusPath(instance), '/api/instances/altinn-studio/0/status');
});

test('converts an applied Tailscale HTTPS route into a WSS remote host', () => {
  const instance = {
    agent: 'altinn-studio',
    instance: 'replica-0',
    status: 'running',
    ready: true,
  };
  const document = {
    status: {
      phase: 'running',
      tailscaleServe: {
        routes: [{
          service: 'agent_host',
          status: 'applied',
          url: 'https://desktop.example.ts.net:18765',
        }],
      },
    },
  };

  assert.deepEqual(discoveredHost(instance, document), {
    key: 'altinn-studio/replica-0',
    address: 'wss://desktop.example.ts.net:18765',
    name: 'AgentDP: altinn-studio/replica-0',
  });
});

test('does not publish instances before both readiness and route application', () => {
  const instance = { agent: 'agent', instance: 'replica-0', status: 'running', ready: false };
  const document = { status: { phase: 'running', tailscaleServe: { routes: [] } } };

  assert.equal(discoveredHost(instance, document), undefined);
});

test('replaces entries in the reserved AgentDP namespace', () => {
  const current = [
    { name: 'Personal host', address: 'wss://personal.example' },
    { name: 'AgentDP: old', address: 'wss://old.example' },
  ];
  const next = [{ key: 'new/replica-0', name: 'AgentDP: new', address: 'wss://new.example' }];

  assert.deepEqual(mergeRemoteHosts(current, next), [
    { name: 'Personal host', address: 'wss://personal.example' },
    { name: 'AgentDP: new', address: 'wss://new.example' },
  ]);
});

test('adopts an existing entry instead of publishing a duplicate address', () => {
  const current = [{ name: 'Temporary manual entry', address: 'wss://agent.example' }];
  const next = [{ key: 'agent/replica-0', name: 'AgentDP: agent/replica-0', address: 'wss://agent.example' }];

  assert.deepEqual(mergeRemoteHosts(current, next), [
    { name: 'AgentDP: agent/replica-0', address: 'wss://agent.example' },
  ]);
});

test('rejects non-WebSocket-compatible route protocols', () => {
  assert.throws(() => toWebSocketUrl('ssh://desktop.example'), /unsupported Agent Host URL protocol/);
});
