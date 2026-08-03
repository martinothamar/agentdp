'use strict';

const assert = require('node:assert/strict');
const Module = require('node:module');
const test = require('node:test');

test('server changes preserve hosts until the latest server reconciles successfully', async () => {
  let serverUrl = 'https://first.example';
  let remoteHosts = [
    { name: 'Personal host', address: 'wss://personal.example' },
    { name: 'AgentDP: previous', address: 'wss://previous.example' },
  ];
  const vscode = {
    ConfigurationTarget: { Global: 1 },
    workspace: {
      getConfiguration(section) {
        if (section === 'agentdp') {
          return { get: () => serverUrl };
        }
        if (section === 'chat') {
          return {
            get: () => remoteHosts,
            update: async (_key, value) => { remoteHosts = value; },
          };
        }
        throw new Error(`unexpected configuration section: ${section}`);
      },
    },
  };
  const originalLoad = Module._load;
  Module._load = function load(request, parent, isMain) {
    return request === 'vscode' ? vscode : originalLoad.call(this, request, parent, isMain);
  };
  const { DiscoveryController } = require('../extension');
  Module._load = originalLoad;

  const lifecycle = [];
  const controller = new DiscoveryController({ appendLine() {} });
  controller.run = (url, signal) => {
    lifecycle.push(`start ${url}`);
    return new Promise(resolve => signal.addEventListener('abort', () => {
      lifecycle.push(`stop ${url}`);
      if (url === 'https://first.example') {
        remoteHosts.push({ name: 'AgentDP: stale', address: 'wss://stale.example' });
      }
      resolve();
    }, { once: true }));
  };

  await controller.restart();
  assert.deepEqual(remoteHosts, [
    { name: 'Personal host', address: 'wss://personal.example' },
    { name: 'AgentDP: previous', address: 'wss://previous.example' },
  ]);
  serverUrl = 'https://second.example';
  const second = controller.restart();
  serverUrl = 'https://latest.example';
  const latest = controller.restart();
  await Promise.all([second, latest]);

  assert.deepEqual(lifecycle, [
    'start https://first.example',
    'stop https://first.example',
    'start https://latest.example',
  ]);
  assert.deepEqual(remoteHosts, [
    { name: 'Personal host', address: 'wss://personal.example' },
    { name: 'AgentDP: previous', address: 'wss://previous.example' },
    { name: 'AgentDP: stale', address: 'wss://stale.example' },
  ]);
  await controller.stop();
  remoteHosts.push({ name: 'AgentDP: stray', address: 'wss://stray.example' });
  serverUrl = '';
  await controller.restart();
  assert.deepEqual(remoteHosts, [{ name: 'Personal host', address: 'wss://personal.example' }]);
});
