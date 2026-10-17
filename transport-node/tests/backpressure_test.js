/*
 * Copyright 2026 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const { describe, it } = require("node:test");
const assert = require("node:assert").strict;
const { createServer } = require("node:net");
const { connect } = require("../index");
const {
  createInbox,
  deadline,
  deferred,
  delay,
} = require("@nats-io/nats-core");
const { NatsServer } = require("@nats-io/nst");

// Enough data to fill the socket's buffer many times over, so that frames
// wait in the transport's backlog
const SIZE = 64 * 1024;
const COUNT = 300;

// A payload that carries its sequence number in its first bytes
function payload(seq) {
  const data = new Uint8Array(SIZE);
  new DataView(data.buffer).setUint32(0, seq);
  data.fill(seq % 251, 4);
  return data;
}

function seqOf(data) {
  return new DataView(data.buffer, data.byteOffset).getUint32(0);
}

// Subscribes to subject and resolves with the received payloads once count
// messages arrived; they are checked by the caller, as an assertion thrown
// in the callback wouldn't reach the test
async function collect(nc, subject, count) {
  const done = deferred();
  const received = [];
  nc.subscribe(subject, {
    callback: (_err, msg) => {
      received.push(msg.data);
      if (received.length === count) {
        done.resolve(received);
      }
    },
  });
  await nc.flush();
  return done;
}

// Resolves at the next reconnect of nc
function nextReconnect(nc) {
  const done = deferred();
  (async () => {
    for await (const status of nc.status()) {
      if (status.type === "reconnect") {
        done.resolve();
        return;
      }
    }
  })().then();
  return done;
}

// Asserts that received holds every payload, intact and in order
function assertInOrder(received) {
  assert.equal(received.length, COUNT);
  received.forEach((data, seq) => {
    assert.equal(data.length, SIZE);
    assert.equal(seqOf(data), seq);
    assert.equal(data[SIZE - 1], seq % 251);
  });
}

// A minimal server that answers the handshake and PINGs until stall(), after
// which it stops reading from the connections it has, so that the client's
// socket buffer fills, or until throttle(), after which it reads them slowly;
// connections made later, such as reconnects, work normally
async function stallingServer() {
  const sockets = [];
  const timers = [];
  let received = 0;
  const server = createServer((socket) => {
    sockets.push(socket);
    socket.write(
      `INFO ${
        JSON.stringify({
          server_id: "stalling",
          version: "2.10.0",
          proto: 1,
          headers: true,
          max_payload: 1024 * 1024,
        })
      }\r\n`,
    );

    // Keeps the end of the last chunk, in case a PING spans two chunks
    let tail = "";
    socket.on("data", (chunk) => {
      if (socket === sockets[0]) {
        received += chunk.length;
      }

      const text = tail + chunk.toString("latin1");
      const pings = text.split("PING\r\n").length - 1;
      tail = text.slice(-5);
      for (let index = 0; index < pings; index++) {
        socket.write("PONG\r\n");
      }
    });
    socket.on("error", () => {});
  });

  const port = await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => resolve(server.address().port));
  });

  return {
    port,
    // Bytes read from the first connection
    received: () => received,
    stall: () => sockets.forEach((socket) => socket.pause()),
    // Reads one chunk every 20ms, slow enough that the client keeps frames
    // in its backlog for seconds, while the connection keeps making progress
    throttle: () =>
      sockets.forEach((socket) => {
        socket.pause();
        timers.push(setInterval(() => {
          socket.once("data", () => socket.pause());
          socket.resume();
        }, 20));
      }),
    // Ends the connections and reads again, so that the client sees a drain
    // after the end
    endAll: () =>
      sockets.forEach((socket) => {
        socket.end();
        socket.resume();
      }),
    stop: () => {
      timers.forEach((timer) => clearInterval(timer));
      sockets.forEach((socket) => socket.destroy());
      return new Promise((resolve) => server.close(resolve));
    },
  };
}

// Publishes enough to the stalled server that frames wait in the backlog
function fillBacklog(nc) {
  for (let seq = 0; seq < COUNT; seq++) {
    nc.publish("stalled", payload(seq));
  }
}

describe(
  "backpressure",
  { timeout: 60_000, concurrency: true, forceExit: true },
  () => {
    it("frames past a full socket buffer arrive in order", async () => {
      const ns = await NatsServer.start();
      const sc = await connect({ port: ns.port });
      const pc = await connect({ port: ns.port });
      const subject = createInbox();
      const received = collect(sc, subject, COUNT);

      for (let seq = 0; seq < COUNT; seq++) {
        pc.publish(subject, payload(seq));
      }

      assertInOrder(await received);
      await pc.close();
      await sc.close();
      await ns.stop();
    });

    it("flush waits for the frames held back", async () => {
      const ns = await NatsServer.start();
      const sc = await connect({ port: ns.port });
      const pc = await connect({ port: ns.port });
      const subject = createInbox();
      const received = collect(sc, subject, COUNT);

      for (let seq = 0; seq < COUNT; seq++) {
        pc.publish(subject, payload(seq));
      }

      // The PING queues behind the backlog, so the PONG means the server
      // already has every message
      await pc.flush();
      assert.equal(pc.stats().outMsgs, COUNT);
      assertInOrder(await received);
      await pc.close();
      await sc.close();
      await ns.stop();
    });

    it("drain sends the frames held back before closing", async () => {
      const ns = await NatsServer.start();
      const sc = await connect({ port: ns.port });
      const pc = await connect({ port: ns.port });
      const subject = createInbox();
      const received = collect(sc, subject, COUNT);

      for (let seq = 0; seq < COUNT; seq++) {
        pc.publish(subject, payload(seq));
      }

      await pc.drain();
      assertInOrder(await received);
      await sc.close();
      await ns.stop();
    });

    it("a reconnect doesn't wait forever on a stalled server", async () => {
      const server = await stallingServer();
      // A short flusherTimeout, as the close gives up after it without progress
      const nc = await connect({
        port: server.port,
        reconnectTimeWait: 100,
        flusherTimeout: 1000,
      });
      const reconnected = nextReconnect(nc);

      server.stall();
      fillBacklog(nc);

      // The transport gives up waiting for the frames it holds back
      await nc.reconnect();
      await deadline(reconnected, 10_000);
      await nc.close();
      await server.stop();
    });

    it("a server that stopped reading fails the connection", async () => {
      const server = await stallingServer();
      const nc = await connect({
        port: server.port,
        reconnect: false,
        flusherTimeout: 1000,
      });

      server.stall();
      fillBacklog(nc);
      const start = Date.now();

      // Like a write that misses its deadline in the Go client, without
      // waiting for the ping interval
      const err = await deadline(nc.closed(), 10_000);
      const waited = Date.now() - start;
      assert.ok(err instanceof Error, `closed with ${err}`);
      assert.match(err.message, /flusher timeout/);
      assert.ok(waited >= 900, `failed after ${waited}ms`);
      await server.stop();
    });

    it("a server that stopped reading fails the connection with little queued", async () => {
      const server = await stallingServer();
      const nc = await connect({
        port: server.port,
        reconnect: false,
        flusherTimeout: 1000,
      });
      const transport = nc.protocol.transport;
      server.stall();

      // A slow publisher: a message below the socket's highWaterMark only once
      // the socket handed everything before it to the kernel, so frames are
      // never held back; once the kernel's buffers are full, the last message
      // stays in the socket, which the flusher watches like the Go client's
      // deadline on every write
      const closed = nc.closed();
      let done = false;
      closed.then(() => done = true);
      const message = new Uint8Array(15 * 1024);
      const until = Date.now() + 10_000;
      while (!done && Date.now() < until) {
        if (transport.socket?.writableLength === 0) {
          nc.publish("stalled", message);
        }

        assert.equal(transport.waitingForDrain, false);
        await delay(1);
      }

      const err = await deadline(closed, 1000);
      assert.ok(err instanceof Error, `closed with ${err}`);
      assert.match(err.message, /flusher timeout/);
      await server.stop();
    });

    it("a slow server doesn't fail the connection", async () => {
      const server = await stallingServer();
      // The transfer below takes longer than this flusherTimeout, but keeps going
      const nc = await connect({ port: server.port, flusherTimeout: 1000 });
      const statuses = [];
      (async () => {
        for await (const status of nc.status()) {
          statuses.push(status.type);
        }
      })().then();

      server.throttle();
      for (let seq = 0; seq < COUNT; seq++) {
        nc.publish("throttled", payload(seq));
      }

      const until = Date.now() + 30_000;
      while (server.received() < COUNT * SIZE && Date.now() < until) {
        await delay(50);
      }
      assert.ok(
        server.received() >= COUNT * SIZE,
        `the server read ${server.received()} of ${COUNT * SIZE} bytes`,
      );
      assert.ok(!statuses.includes("disconnect"), `statuses: ${statuses}`);
      assert.equal(nc.isClosed(), false);
      await nc.close();
      await server.stop();
    });

    it("a close accepts a flusherTimeout longer than a timer", async () => {
      const server = await stallingServer();
      const nc = await connect({
        port: server.port,
        flusherTimeout: Number.MAX_SAFE_INTEGER,
      });
      const transport = nc.protocol.transport;
      // Other tests run alongside this one, so only overflows count
      const warnings = [];
      const onWarning = (warning) => {
        if (warning.name === "TimeoutOverflowWarning") {
          warnings.push(warning.message);
        }
      };
      process.on("warning", onWarning);

      server.stall();
      fillBacklog(nc);

      // The close waits for the stalled server without a timer that
      // overflows, which would fire at once and warn on every check
      await nc.reconnect();
      await delay(300);
      process.off("warning", onWarning);
      assert.equal(transport.isClosed, false);
      assert.deepEqual(warnings, []);
      await nc.close();
      await server.stop();
    });

    it("a reconnect waits for a slow server to take what was sent", async () => {
      const server = await stallingServer();
      // The transfer below takes longer than this flusherTimeout, but keeps going
      const nc = await connect({
        port: server.port,
        reconnectTimeWait: 100,
        flusherTimeout: 1000,
      });
      const reconnected = nextReconnect(nc);

      server.throttle();
      for (let seq = 0; seq < COUNT; seq++) {
        nc.publish("throttled", payload(seq));
      }

      await nc.reconnect();
      await deadline(reconnected, 30_000);

      // The flush hands frames to the network; the server keeps reading them
      // after the reconnect, and gets all of them only if none were dropped
      const until = Date.now() + 30_000;
      while (server.received() < COUNT * SIZE && Date.now() < until) {
        await delay(50);
      }
      assert.ok(
        server.received() >= COUNT * SIZE,
        `the server read ${server.received()} of ${COUNT * SIZE} bytes`,
      );
      await nc.close();
      await server.stop();
    });

    it("a server ending the connection closes it cleanly", async () => {
      const server = await stallingServer();
      const nc = await connect({ port: server.port, reconnect: false });
      const transport = nc.protocol.transport;

      server.stall();
      fillBacklog(nc);
      server.endAll();

      // The frames held back are dropped rather than written after the end,
      // which would turn the close into an error; when that would happen
      // depends on timing, so this only checks that the close is clean
      await deadline(nc.closed(), 10_000);
      assert.equal(transport.closeError, undefined);
      await server.stop();
    });

    it("a close counts from the last data the server took", async () => {
      const server = await stallingServer();
      const nc = await connect({
        port: server.port,
        reconnectTimeWait: 100,
        flusherTimeout: 1000,
      });
      const transport = nc.protocol.transport;

      server.stall();
      fillBacklog(nc);

      // The server took its last data well before the close starts, so the
      // close gives up a flusherTimeout after that, not after its own start
      await delay(600);
      const start = Date.now();
      await nc.reconnect();
      await deadline(transport.closed(), 10_000);
      const waited = Date.now() - start;
      assert.ok(waited < 900, `the close waited ${waited}ms`);
      await nc.close();
      await server.stop();
    });

    it("a close after an idle spell waits for what was just sent", async () => {
      const ns = await NatsServer.start();
      const sc = await connect({ port: ns.port });
      const pc = await connect({ port: ns.port, flusherTimeout: 1000 });
      const subject = createInbox();
      const received = collect(sc, subject, COUNT);

      // Nothing was written for longer than the flusherTimeout, which isn't a
      // lack of progress, as there was nothing to write
      await delay(1500);
      for (let seq = 0; seq < COUNT; seq++) {
        pc.publish(subject, payload(seq));
      }
      await pc.reconnect();

      assertInOrder(await deadline(received, 10_000));
      await pc.close();
      await sc.close();
      await ns.stop();
    });

    it("frames sent during a close don't hold it up", async () => {
      const server = await stallingServer();
      const nc = await connect({
        port: server.port,
        reconnectTimeWait: 100,
        flusherTimeout: 1000,
      });
      const transport = nc.protocol.transport;

      server.throttle();
      fillBacklog(nc);

      // The server keeps taking data, but slower than it is published, so the
      // backlog never empties; the close waits only for the frames held back
      // when it started
      await nc.reconnect();
      const publisher = setInterval(() => {
        nc.publish("throttled", payload(0));
      }, 10);
      try {
        await deadline(transport.closed(), 30_000);
      } finally {
        clearInterval(publisher);
      }

      const until = Date.now() + 30_000;
      while (server.received() < COUNT * SIZE && Date.now() < until) {
        await delay(50);
      }
      assert.ok(
        server.received() >= COUNT * SIZE,
        `the server read ${server.received()} of ${COUNT * SIZE} bytes`,
      );
      await nc.close();
      await server.stop();
    });

    it("a publish while a reconnect starts is still written", async () => {
      const ns = await NatsServer.start();
      const nc = await connect({ port: ns.port });
      const socket = nc.protocol.transport.socket;

      // The old connection is still open until its close finished, so a
      // publish meanwhile goes to its socket rather than being dropped;
      // whether the server gets it depends on the close, so this only checks
      // that it is written
      await nc.reconnect();
      const frames = [];
      const write = socket.write.bind(socket);
      socket.write = (frame, ...args) => {
        frames.push(frame);
        return write(frame, ...args);
      };
      nc.publish("closing", payload(0));
      // The publish is handed to the transport in a microtask
      await Promise.resolve();

      assert.ok(frames.some((frame) => frame.length > SIZE));
      await nc.close();
      await ns.stop();
    });

    it("frames sent after the socket ended aren't written", async () => {
      const server = await stallingServer();
      const nc = await connect({ port: server.port, reconnect: false });
      const socket = nc.protocol.transport.socket;
      const errors = [];
      socket.on("error", (err) => errors.push(err));

      // Once the server ends the connection, the end handler ends the socket;
      // "finish" comes after that and before the connection closes, so a
      // publish then finds a socket that can't be written
      let writes = -1;
      socket.once("finish", () => {
        writes = 0;
        const write = socket.write.bind(socket);
        socket.write = (...args) => {
          writes++;
          return write(...args);
        };
        nc.publish("ended", payload(0));
      });

      server.endAll();
      await deadline(nc.closed(), 10_000);
      assert.equal(writes, 0);
      assert.deepEqual(errors, []);
      await server.stop();
    });

    it("a discarded transport stops watching and drops the backlog", async () => {
      const server = await stallingServer();
      const nc = await connect({ port: server.port, reconnect: false });
      const transport = nc.protocol.transport;

      server.stall();
      fillBacklog(nc);
      assert.notEqual(transport.flusherWatch, undefined);

      transport.discard();
      assert.equal(transport.flusherWatch, undefined);
      assert.equal(transport.backlog.length - transport.backlogStart, 0);
      await nc.close();
      await server.stop();
    });

    it("close doesn't wait for the frames held back", async () => {
      const server = await stallingServer();
      const nc = await connect({ port: server.port });

      server.stall();
      fillBacklog(nc);

      await deadline(nc.close(), 10_000);
      await server.stop();
    });
  },
);
