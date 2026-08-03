'use strict';

const AGENT_HOST_SERVICE = 'agent_host';
const AGENTDP_HOST_PREFIX = 'AgentDP: ';

function instanceKey(instance) {
  return `${instance.agent}/${instance.instance}`;
}

function instanceStatusPath(instance) {
  return `/api/instances/${encodeURIComponent(instance.agent)}/${instance.instance_id}/status`;
}

function toWebSocketUrl(value) {
  const url = new URL(value);
  if (url.protocol === 'https:') {
    url.protocol = 'wss:';
  } else if (url.protocol === 'http:') {
    url.protocol = 'ws:';
  } else if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error(`unsupported Agent Host URL protocol: ${url.protocol}`);
  }
  return url.toString().replace(/\/$/, '');
}

function discoveredHost(instance, document) {
  if (instance.status !== 'running' || instance.ready !== true || document?.status?.phase !== 'running') {
    return undefined;
  }
  const route = document.status.tailscaleServe?.routes?.find(candidate =>
    candidate.service === AGENT_HOST_SERVICE && candidate.status === 'applied'
  );
  if (!route?.url) {
    return undefined;
  }
  const key = instanceKey(instance);
  return {
    key,
    address: toWebSocketUrl(route.url),
    name: `${AGENTDP_HOST_PREFIX}${key}`,
  };
}

function mergeRemoteHosts(current, nextManaged) {
  const nextAddresses = new Set(nextManaged.map(host => host.address));
  const unmanaged = current.filter(host =>
    !host.name?.startsWith(AGENTDP_HOST_PREFIX) && !nextAddresses.has(host.address)
  );
  const next = nextManaged
    .map(({ address, name }) => ({ address, name }))
    .sort((left, right) => left.name.localeCompare(right.name));
  return [...unmanaged, ...next];
}

module.exports = {
  AGENTDP_HOST_PREFIX,
  discoveredHost,
  instanceKey,
  instanceStatusPath,
  mergeRemoteHosts,
  toWebSocketUrl,
};
