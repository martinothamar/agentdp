#!/usr/bin/env node
"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const net = require("node:net");
const tls = require("node:tls");

const target = new URL(process.env.AGENTDP_NETWORK_WSS_URL || "wss://ws.postman-echo.com/raw");
const timeoutMs = Number(process.env.AGENTDP_NETWORK_WSS_TIMEOUT_MS || 20000);
const message = `agentdp-ws-smoke-${Date.now()}`;

if (target.protocol !== "wss:") {
  throw new Error(`expected wss URL, got ${target.href}`);
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exit(1);
});

async function main() {
  const targetPort = Number(target.port || 443);
  const authority = `${target.hostname}:${targetPort}`;
  const socket = await connectTcp(target.hostname, targetPort);
  const secureSocket = await connectTls(socket, target.hostname);
  const wsReader = new StreamReader(secureSocket);
  const key = crypto.randomBytes(16).toString("base64");
  const path = `${target.pathname || "/"}${target.search}`;

  secureSocket.write(
    [
      `GET ${path} HTTP/1.1`,
      `Host: ${authority}`,
      "Connection: Upgrade",
      "Upgrade: websocket",
      `Sec-WebSocket-Key: ${key}`,
      "Sec-WebSocket-Version: 13",
      "",
      "",
    ].join("\r\n"),
  );

  const upgradeResponse = await wsReader.readUntil(Buffer.from("\r\n\r\n"));
  const upgradeText = upgradeResponse.toString("utf8");
  if (!upgradeText.startsWith("HTTP/1.1 101")) {
    throw new Error(`websocket upgrade failed:\n${upgradeText}`);
  }
  assertAcceptHeader(upgradeText, key);

  writeFrame(secureSocket, 0x1, Buffer.from(message, "utf8"));
  const echo = await readTextFrame(wsReader, secureSocket);
  if (echo !== message) {
    throw new Error(`unexpected websocket echo: ${JSON.stringify(echo)}`);
  }

  writeFrame(secureSocket, 0x8, Buffer.alloc(0));
  secureSocket.end();
  console.log(`network-websocket-smoke-ok ${target.href}`);
}

function connectTcp(host, port) {
  return withTimeout(
    new Promise((resolve, reject) => {
      const socket = net.createConnection({ host, port }, () => {
        socket.setNoDelay(true);
        resolve(socket);
      });
      socket.once("error", reject);
    }),
    `TCP connect to ${host}:${port}`,
  );
}

function connectTls(socket, servername) {
  return withTimeout(
    new Promise((resolve, reject) => {
      const secureSocket = tls.connect({
        socket,
        servername,
        ca: caBundle(),
        ALPNProtocols: ["http/1.1"],
      });
      secureSocket.once("secureConnect", () => resolve(secureSocket));
      secureSocket.once("error", reject);
    }),
    `TLS connect to ${servername}`,
  );
}

function caBundle() {
  for (const path of [
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/ca-certificates/extracted/tls-ca-bundle.pem",
  ]) {
    if (fs.existsSync(path)) {
      return fs.readFileSync(path);
    }
  }
  return undefined;
}

function assertAcceptHeader(response, key) {
  const expected = crypto
    .createHash("sha1")
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64");
  const found = response
    .split("\r\n")
    .find((line) => line.toLowerCase().startsWith("sec-websocket-accept:"))
    ?.split(":")
    .slice(1)
    .join(":")
    .trim();
  if (found !== expected) {
    throw new Error(`invalid Sec-WebSocket-Accept: expected ${expected}, got ${found || "<missing>"}`);
  }
}

async function readTextFrame(reader, socket) {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const frame = await readFrame(reader);
    if (frame.opcode === 0x1) {
      return frame.payload.toString("utf8");
    }
    if (frame.opcode === 0x8) {
      throw new Error("websocket closed before echo");
    }
    if (frame.opcode === 0x9) {
      writeFrame(socket, 0xa, frame.payload);
    }
  }
  throw new Error("did not receive websocket text frame");
}

async function readFrame(reader) {
  const header = await reader.read(2);
  const opcode = header[0] & 0x0f;
  const masked = (header[1] & 0x80) !== 0;
  let length = header[1] & 0x7f;

  if (length === 126) {
    length = (await reader.read(2)).readUInt16BE(0);
  } else if (length === 127) {
    const extended = await reader.read(8);
    const high = extended.readUInt32BE(0);
    const low = extended.readUInt32BE(4);
    length = high * 2 ** 32 + low;
  }

  const mask = masked ? await reader.read(4) : undefined;
  const payload = await reader.read(length);
  if (mask) {
    for (let index = 0; index < payload.length; index += 1) {
      payload[index] ^= mask[index % 4];
    }
  }
  return { opcode, payload };
}

function writeFrame(socket, opcode, payload) {
  const mask = crypto.randomBytes(4);
  const header = [];
  header.push(0x80 | opcode);
  if (payload.length < 126) {
    header.push(0x80 | payload.length);
  } else if (payload.length <= 0xffff) {
    header.push(0x80 | 126, (payload.length >> 8) & 0xff, payload.length & 0xff);
  } else {
    throw new Error("payload too large for smoke test");
  }

  const masked = Buffer.from(payload);
  for (let index = 0; index < masked.length; index += 1) {
    masked[index] ^= mask[index % 4];
  }
  socket.write(Buffer.concat([Buffer.from(header), mask, masked]));
}

class StreamReader {
  constructor(stream) {
    this.stream = stream;
    this.buffer = Buffer.alloc(0);
    this.waiter = undefined;
    this.onData = (chunk) => {
      this.buffer = Buffer.concat([this.buffer, chunk]);
      this.wake();
    };
    this.onEnd = () => this.wake(new Error("stream ended"));
    this.onError = (error) => this.wake(error);
    stream.on("data", this.onData);
    stream.once("end", this.onEnd);
    stream.once("error", this.onError);
  }

  detach() {
    this.stream.off("data", this.onData);
    this.stream.off("end", this.onEnd);
    this.stream.off("error", this.onError);
  }

  async read(length) {
    return withTimeout(this.waitFor(() => this.buffer.length >= length).then(() => this.consume(length)), "read");
  }

  async readUntil(marker) {
    return withTimeout(
      this.waitFor(() => this.buffer.indexOf(marker) >= 0).then(() => {
        const end = this.buffer.indexOf(marker) + marker.length;
        return this.consume(end);
      }),
      "read headers",
    );
  }

  consume(length) {
    const value = this.buffer.subarray(0, length);
    this.buffer = this.buffer.subarray(length);
    return value;
  }

  waitFor(predicate) {
    if (predicate()) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      this.waiter = { predicate, resolve, reject };
    });
  }

  wake(error) {
    const waiter = this.waiter;
    if (!waiter) {
      return;
    }
    if (error) {
      this.waiter = undefined;
      waiter.reject(error);
      return;
    }
    if (waiter.predicate()) {
      this.waiter = undefined;
      waiter.resolve();
    }
  }
}

function withTimeout(promise, label) {
  let timeout;
  const timer = new Promise((_, reject) => {
    timeout = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
  });
  return Promise.race([promise, timer]).finally(() => clearTimeout(timeout));
}
