/*
 * Copyright 2020-2024 The NATS Authors
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
import type { ConnectionOptions } from "./nats-base-client";
import {
  checkOptions,
  DataBuffer,
  DEFAULT_FLUSHER_TIMEOUT,
  Deferred,
  deferred,
  delay,
  errors,
  extend,
  extractProtocolMessage,
  INFO,
  render,
  ServerInfo,
  Transport,
} from "./nats-base-client";
import { createConnection, Socket } from "node:net";
import { connect as tlsConnect, TlsOptions, TLSSocket } from "node:tls";
import { resolve } from "node:path";
import { existsSync, readFile } from "node:fs";
import dns from "node:dns";
import { Buffer } from "node:buffer";
import { version } from "./version";
import type { NodeConnectionOptions } from "./connect";

export const VERSION = version;
const LANG = "nats.js";
// The flusher watch and a close check this many times per flusherTimeout
// whether the socket took any data, so that they act close to a
// flusherTimeout after the last progress
const PROGRESS_CHECKS_PER_TIMEOUT = 10;
// The longest delay setTimeout takes; a longer one fires at once
const MAX_TIMER_DELAY = 2 ** 31 - 1;

export class NodeTransport implements Transport {
  socket!: Socket;
  version: string;
  lang: string;
  yields: Uint8Array[] = [];
  signal: Deferred<void> = deferred<void>();
  closedNotification: Deferred<void | Error> = deferred();
  options!: ConnectionOptions;
  connected = false;
  tlsName = "";
  done = false;
  closeError?: Error;

  // Frames held back while the socket's buffer is full, written in order on
  // "drain"; writing past a full buffer grows the socket's own queue without
  // bound, which some runtimes, such as Bun, handle far slower than Node;
  // frames before backlogStart are already written
  backlog: Uint8Array[] = [];
  backlogStart = 0;
  // Set while the socket's buffer is full and a "drain" is awaited
  waitingForDrain = false;
  // Resolved once the frames held back when a close started are written, for
  // a close that must send them first; frames sent later don't hold it up
  backlogWritten?: Deferred<void>;
  // How many of those frames are still in the backlog
  framesBeforeClose = 0;
  // When the socket last handed written data to the network or had nothing
  // to hand, as an idle socket isn't one that stopped making progress; read
  // from the monotonic clock, so that a change of the system time doesn't
  // look like progress or a lack of it
  lastProgress = performance.now();
  // The one "drain" listener, so that dropping the backlog can remove it
  private readonly onDrain = () => this.writeBacklog();
  // Runs while the socket has data it hasn't handed to the network, and fails
  // the connection once it took none for flusherTimeout, like a write that
  // misses its deadline in the Go client
  private flusherWatch?: ReturnType<typeof setTimeout>;
  // Set once a close started, which bounds its own wait, so that the flusher
  // watch doesn't start again and fail it
  private closeStarted = false;

  constructor() {
    this.lang = LANG;
    this.version = VERSION;
  }
  async connect(
    hp: { hostname: string; port: number; tlsName: string },
    options: NodeConnectionOptions,
  ): Promise<void> {
    this.tlsName = hp.tlsName;
    this.options = options;
    const { tls } = this.options;
    const { handshakeFirst } = tls || {};
    try {
      if (handshakeFirst === true) {
        this.socket = await this.tlsFirst(hp);
      } else {
        this.socket = await this.dial(hp);
      }
      // protocol could have terminated
      if (this.done) {
        this.socket?.destroy();
      }

      const info = await this.peekInfo();
      checkOptions(info, options);
      const { tls_required: tlsRequired, tls_available: tlsAvailable } = info;
      const desired = tlsAvailable === true && options.tls !== null;
      if (!handshakeFirst && (tlsRequired || desired)) {
        this.socket = await this.startTLS();
      }

      if (this.done) {
        this.socket?.destroy();
      }

      //@ts-ignore: this is possibly a TlsSocket
      if (tlsRequired && this.socket.encrypted !== true) {
        throw errors.InvalidArgumentError.format(
          "tls",
          "is not available on this server",
        );
      }

      this.connected = true;
      this.setupHandlers();
      this.signal.resolve();
      return Promise.resolve();
    } catch (ex) {
      let err = ex;
      if (!err) {
        // this seems to be possible in Kubernetes
        // where an error is thrown, but it is undefined
        // when something like istio-init is booting up
        err = new errors.ConnectionError(
          "error connecting - node provided an undefined error",
        );
      }
      // @ts-ignore: node error
      const { code } = err;
      const perr = code === "ECONNREFUSED"
        ? new errors.ConnectionError("connection refused", { cause: err })
        : err;
      this.socket?.destroy();
      throw perr;
    }
  }

  dial(hp: { hostname: string; port: number }): Promise<Socket> {
    const d = deferred<Socket>();
    let dialError: Error;
    const socket = createConnection(hp.port, hp.hostname, () => {
      d.resolve(socket);
      socket.removeAllListeners();
    });
    socket.on("error", (err) => {
      dialError = err;
    });
    socket.on("close", () => {
      socket.removeAllListeners();
      d.reject(dialError);
    });
    socket.setNoDelay(true);
    return d;
  }

  get isClosed(): boolean {
    return this.done;
  }

  close(err?: Error): Promise<void> {
    return this._closed(err, false);
  }

  peekInfo(): Promise<ServerInfo> {
    const d = deferred<ServerInfo>();
    let peekError: Error;
    this.socket.on("data", (frame: Uint8Array) => {
      this.yields.push(frame);
      const t = DataBuffer.concat(...this.yields);
      const pm = extractProtocolMessage(t);
      if (pm !== "") {
        try {
          const m = INFO.exec(pm);
          if (!m) {
            throw new Error("unexpected response from server");
          }
          const info = JSON.parse(m[1]);
          d.resolve(info);
        } catch (err) {
          d.reject(err);
        } finally {
          this.socket.removeAllListeners();
        }
      }
    });
    this.socket.on("error", (err) => {
      peekError = err;
    });
    this.socket.on("close", () => {
      this.socket.removeAllListeners();
      d.reject(peekError);
    });

    return d;
  }

  loadFile(fn: string): Promise<Buffer | void> {
    if (!fn) {
      return Promise.resolve();
    }
    const d = deferred<Buffer | void>();
    try {
      fn = resolve(fn);
      if (!existsSync(fn)) {
        d.reject(new Error(`${fn} doesn't exist`));
      }
      readFile(fn, (err, data) => {
        if (err) {
          return d.reject(err);
        }
        d.resolve(data);
      });
    } catch (err) {
      d.reject(err);
    }
    return d;
  }

  async loadClientCerts(): Promise<TlsOptions | void> {
    const tlsOpts = {} as TlsOptions;
    const { certFile, cert, caFile, ca, keyFile, key } = this.options.tls;
    try {
      if (certFile) {
        const data = await this.loadFile(certFile);
        if (data) {
          tlsOpts.cert = data;
        }
      } else if (cert) {
        tlsOpts.cert = cert;
      }
      if (keyFile) {
        const data = await this.loadFile(keyFile);
        if (data) {
          tlsOpts.key = data;
        }
      } else if (key) {
        tlsOpts.key = key;
      }
      if (caFile) {
        const data = await this.loadFile(caFile);
        if (data) {
          tlsOpts.ca = [data];
        }
      } else if (ca) {
        tlsOpts.ca = ca;
      }
      return Promise.resolve(tlsOpts);
    } catch (err) {
      return Promise.reject(err);
    }
  }

  async tlsFirst(hp: { hostname: string; port: number }): Promise<TLSSocket> {
    let tlsError: Error;
    let tlsOpts: {
      rejectUnauthorized: boolean;
      servername: string;
      socket?: Socket;
    } = {
      servername: this.tlsName,
      rejectUnauthorized: true,
    };
    if (this.socket) {
      tlsOpts.socket = this.socket;
    }
    if (typeof this.options.tls === "object") {
      try {
        const certOpts = await this.loadClientCerts() || {};
        tlsOpts = extend(tlsOpts, this.options.tls, certOpts);
      } catch (err) {
        return Promise.reject(
          new errors.ConnectionError((err as Error).message, { cause: err }),
        );
      }
    }
    const d = deferred<TLSSocket>();
    try {
      const tlsSocket = tlsConnect(hp.port, hp.hostname, tlsOpts, () => {
        tlsSocket.removeAllListeners();
        d.resolve(tlsSocket);
      });

      tlsSocket.on("error", (err) => {
        tlsError = err;
      });
      tlsSocket.on("secureConnect", () => {
        // socket won't be authorized, if the user disabled it
        if (tlsOpts.rejectUnauthorized === false) {
          return;
        }
        if (!tlsSocket.authorized) {
          throw tlsSocket.authorizationError;
        }
      });
      tlsSocket.on("close", () => {
        d.reject(tlsError);
        tlsSocket.removeAllListeners();
      });
      tlsSocket.setNoDelay(true);
    } catch (err) {
      // tls throws errors on bad certs see nats.js#310
      d.reject(
        new errors.ConnectionError((err as Error).message, { cause: err }),
      );
    }
    return d;
  }

  async startTLS(): Promise<TLSSocket> {
    let tlsError: Error;
    let tlsOpts = {
      socket: this.socket,
      servername: this.tlsName,
      rejectUnauthorized: true,
    };
    if (typeof this.options.tls === "object") {
      try {
        const certOpts = await this.loadClientCerts() || {};
        tlsOpts = extend(tlsOpts, this.options.tls, certOpts);
      } catch (err) {
        return Promise.reject(
          new errors.ConnectionError((err as Error).message, {
            cause: err,
          }),
        );
      }
    }
    const d = deferred<TLSSocket>();
    try {
      const tlsSocket = tlsConnect(tlsOpts, () => {
        tlsSocket.removeAllListeners();
        d.resolve(tlsSocket);
      });
      tlsSocket.on("error", (err) => {
        tlsError = err;
      });
      tlsSocket.on("secureConnect", () => {
        // socket won't be authorized, if the user disabled it
        if (tlsOpts.rejectUnauthorized === false) {
          return;
        }
        if (!tlsSocket.authorized) {
          throw tlsSocket.authorizationError;
        }
      });
      tlsSocket.on("close", () => {
        d.reject(tlsError);
        tlsSocket.removeAllListeners();
      });
    } catch (err) {
      // tls throws errors on bad certs see nats.js#310
      d.reject(
        new errors.ConnectionError((err as Error).message, { cause: err }),
      );
    }
    return d;
  }

  setupHandlers() {
    let connError: Error;
    this.socket.on("data", (frame: Uint8Array) => {
      this.yields.push(frame);
      return this.signal.resolve();
    });
    this.socket.on("error", (err) => {
      connError = err;
    });

    this.socket.on("end", () => {
      if (this.socket?.destroyed) {
        return;
      }

      // The server ended the connection, so what is held back can't be sent,
      // and writing it after end() would fail
      this.dropBacklog();
      this.socket?.write(new Uint8Array(0), () => {
        this.socket?.end();
      });
    });

    this.socket.on("close", () => {
      this._closed(connError, false);
    });
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    return this.iterate();
  }

  async *iterate(): AsyncIterableIterator<Uint8Array> {
    while (true) {
      if (this.yields.length === 0) {
        await this.signal;
      }
      const yields = this.yields;
      this.yields = [];

      for (let i = 0; i < yields.length; i++) {
        if (this.options.debug) {
          console.info(`> ${render(yields[i])}`);
        }
        yield yields[i];
      }
      // yielding could have paused and microtask
      // could have added messages. Prevent allocations
      // if possible
      if (this.done) {
        break;
      } else if (this.yields.length === 0) {
        yields.length = 0;
        this.yields = yields;
        this.signal = deferred();
      }
    }
  }

  discard() {
    this.done = true;
    this.dropBacklog();
    if (this.socket) {
      try {
        this.socket.removeAllListeners();
        this.socket.destroy();
      } catch {
        // We tried, just ignore it
      }
    }
  }

  disconnect(): void {
    this._closed(undefined, true).then().catch();
  }

  isEncrypted(): boolean {
    return this.socket instanceof TLSSocket;
  }

  // Writes frame directly, ahead of any backlog, and resolves once it is
  // written; only for the close path, after flushBacklog()
  _send(frame: Uint8Array): Promise<void> {
    if (this.isClosed || this.socket === undefined) {
      return Promise.resolve();
    }
    if (this.options.debug) {
      console.info(`< ${render(frame)}`);
    }
    const d = deferred<void>();
    try {
      this.socket.write(frame, (err: Error | undefined) => {
        if (err) {
          this.debugWriteError(frame, err);
          return d.reject(err);
        }
        return d.resolve();
      });
    } catch (err) {
      this.debugWriteError(frame, err);
      d.reject(err);
    }
    return d;
  }

  send(frame: Uint8Array): void {
    if (!this.canWrite()) {
      return;
    }

    if (this.waitingForDrain) {
      this.backlog.push(frame);
      return;
    }

    this.write(frame);
  }

  // Writes frame and returns false once the socket's buffer is full, after
  // which frames wait in the backlog until the next "drain"; write errors are
  // ignored here, as the client fails on a read, the flusher watch fails a
  // connection that stopped taking data, and the heartbeat timer detects a
  // stale one
  private write(frame: Uint8Array): boolean {
    if (this.options.debug) {
      console.info(`< ${render(frame)}`);
    }

    if (this.socket.writableLength === 0) {
      this.lastProgress = performance.now();
    }

    let hasRoom = true;
    try {
      hasRoom = this.socket.write(frame, (err: Error | null | undefined) => {
        if (err) {
          this.debugWriteError(frame, err);
        } else {
          this.lastProgress = performance.now();
        }
      });
    } catch (err) {
      this.debugWriteError(frame, err);
    }

    if (!hasRoom) {
      this.waitingForDrain = true;
      this.socket.once("drain", this.onDrain);
    }

    // Like the Go client's deadline on every write, any data the socket
    // couldn't hand to the network right away is watched
    if (this.socket.writableLength > 0) {
      this.watchFlusher();
    }

    return hasRoom;
  }

  private flusherTimeout(): number {
    return this.options.flusherTimeout ?? DEFAULT_FLUSHER_TIMEOUT;
  }

  // True while frames are held back or the socket holds data it hasn't
  // handed to the network
  private hasUnsentData(): boolean {
    if (this.waitingForDrain) {
      return true;
    }

    if (this.socket === undefined) {
      return false;
    }

    return this.socket.writableLength > 0;
  }

  // Starts the flusher watch unless it runs already or a close started; it
  // stops on its own once nothing is left unsent
  private watchFlusher(): void {
    if (this.flusherWatch !== undefined) {
      return;
    }

    if (this.closeStarted) {
      return;
    }

    const timeout = this.flusherTimeout();
    const interval = Math.min(
      timeout / PROGRESS_CHECKS_PER_TIMEOUT,
      MAX_TIMER_DELAY,
    );

    const check = () => {
      this.flusherWatch = undefined;
      if (this.socket === undefined) {
        return;
      }

      if (!this.hasUnsentData()) {
        return;
      }

      const stalledFor = performance.now() - this.lastProgress;
      if (stalledFor >= timeout) {
        // The "close" that follows closes the transport with this error, and
        // the client reconnects
        this.socket.destroy(
          new errors.ConnectionError(
            `flusher timeout: the server took no data for ${timeout}ms`,
          ),
        );
        return;
      }

      this.flusherWatch = setTimeout(check, interval);
    };

    this.flusherWatch = setTimeout(check, interval);
  }

  private stopFlusherWatch(): void {
    if (this.flusherWatch !== undefined) {
      clearTimeout(this.flusherWatch);
      this.flusherWatch = undefined;
    }
  }

  // Logs a frame that couldn't be written, when debugging
  private debugWriteError(frame: Uint8Array, err: unknown): void {
    if (this.options.debug) {
      console.error(`< ${render(frame)} failed: ${err}`);
    }
  }

  // False once the socket can't take frames: it is gone or the server ended
  // it, and writing after end() fails
  private canWrite(): boolean {
    if (this.isClosed || this.socket === undefined) {
      return false;
    }

    return !this.socket.writableEnded;
  }

  private writeBacklog(): void {
    this.waitingForDrain = false;
    if (!this.canWrite()) {
      this.dropBacklog();
      return;
    }

    const frames = this.backlog;
    while (this.backlogStart < frames.length) {
      const frame = frames[this.backlogStart++];
      const hasRoom = this.write(frame);
      this.frameWrittenBeforeClose();

      if (!hasRoom) {
        // The rest waits for the next drain; drop what was written once it is
        // most of the array, so that each frame is copied about once
        if (this.backlogStart * 2 > frames.length) {
          this.backlog = frames.slice(this.backlogStart);
          this.backlogStart = 0;
        }
        return;
      }
    }

    this.clearBacklog();
  }

  // Counts a frame a close waits for, and releases the close after the last
  private frameWrittenBeforeClose(): void {
    if (!this.backlogWritten) {
      return;
    }

    this.framesBeforeClose--;
    if (this.framesBeforeClose === 0) {
      this.backlogWritten.resolve();
      this.backlogWritten = undefined;
    }
  }

  private clearBacklog(): void {
    this.backlog = [];
    this.backlogStart = 0;
  }

  // Forgets the frames held back, for a socket that can't take them anymore,
  // and releases a close waiting for them
  private dropBacklog(): void {
    this.stopFlusherWatch();
    this.socket?.off("drain", this.onDrain);
    this.clearBacklog();
    this.waitingForDrain = false;
    this.backlogWritten?.resolve();
    this.backlogWritten = undefined;
  }

  // Resolves once sent settles, or once the socket took no data for idle ms,
  // counting from the last data it took; a peer that stopped reading would
  // never let sent settle, while a slow one keeps taking data
  private async untilSentOrStalled(
    sent: Promise<void>,
    idle: number,
  ): Promise<void> {
    let settled = false;
    const finished = sent.then(
      () => settled = true,
      () => settled = true,
    );

    const interval = Math.min(
      idle / PROGRESS_CHECKS_PER_TIMEOUT,
      MAX_TIMER_DELAY,
    );
    while (!settled) {
      const stalledFor = performance.now() - this.lastProgress;
      if (stalledFor >= idle) {
        return;
      }

      const timer = delay(interval);
      await Promise.race([finished, timer]);
      timer.cancel();
    }
  }

  // Resolves once every frame passed to send() so far has been handed to the
  // socket
  private flushBacklog(): Promise<void> {
    // A full socket buffer with nothing held back needs no wait here, as the
    // close's own write goes after what the socket holds
    if (this.backlogStart === this.backlog.length) {
      return Promise.resolve();
    }

    if (!this.backlogWritten) {
      this.framesBeforeClose = this.backlog.length - this.backlogStart;
      this.backlogWritten = deferred<void>();
    }

    return this.backlogWritten;
  }

  private async _closed(err?: Error, internal = true): Promise<void> {
    // if this connection didn't succeed, then ignore it.
    if (!this.connected) return;
    if (this.done) {
      this.socket?.destroy();
      return;
    }
    this.closeError = err;
    // A close bounds its own wait, and reports no error for it
    this.closeStarted = true;
    this.stopFlusherWatch();
    // only try to flush the outbound buffer if we got no error and
    // the close is internal, if the transport closed, we are done.
    if (!err && this.socket && internal) {
      const flushed = this.flushBacklog()
        .then(() => this._send(new Uint8Array(0)));
      // If the wait gives up first, a later write error must not go unhandled
      flushed.catch((err) => {
        if (this.options.debug) {
          console.log("transport close terminated with an error", err);
        }
      });

      await this.untilSentOrStalled(
        flushed,
        this.flusherTimeout(),
      );
    }
    try {
      if (this.socket) {
        this.socket.removeAllListeners();
        this.socket?.destroy();
        this.socket = undefined;
      }
    } catch (err) {
      console.log(err);
    }

    // The socket is gone, so a backlog that wasn't written never will be;
    // don't leave a close waiting for it
    this.dropBacklog();

    this.done = true;
    this.closedNotification.resolve(this.closeError);
  }

  closed(): Promise<void | Error> {
    return this.closedNotification;
  }
}

export function nodeResolveHost(s: string): Promise<string[]> {
  const ips = deferred<string[]>();

  dns.lookup(
    s,
    { all: true },
    //@ts-ignore: callback changes shape when all is true
    (err: Error, address: { address: string; family: number }[]) => {
      if (err) {
        ips.reject(err);
        return;
      }
      const buf = [];
      for (const r of address) {
        buf.push(r.address);
      }
      if (buf.length === 0) {
        buf.push(s);
      }
      ips.resolve(buf);
    },
  );

  return ips;
}
