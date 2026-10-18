// deno-lint-ignore-file no-undef
/*
 * Copyright 2020-2021 The NATS Authors
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
import { DenoBuffer } from "./denobuffer.ts";
import { TD } from "./encoders.ts";
import type { Dispatcher } from "./core.ts";

export const Kind = {
  OK: 0,
  ERR: 1,
  MSG: 2,
  INFO: 3,
  PING: 4,
  PONG: 5,
} as const;

export type Kind = typeof Kind[keyof typeof Kind];

export interface ParserEvent {
  kind: Kind;
  msg?: MsgArg;
  data?: Uint8Array;
}

export function describe(e: ParserEvent): string {
  let ks: string;
  let data = "";

  switch (e.kind) {
    case Kind.MSG:
      ks = "MSG";
      break;
    case Kind.OK:
      ks = "OK";
      break;
    case Kind.ERR:
      ks = "ERR";
      data = TD.decode(e.data);
      break;
    case Kind.PING:
      ks = "PING";
      break;
    case Kind.PONG:
      ks = "PONG";
      break;
    case Kind.INFO:
      ks = "INFO";
      data = TD.decode(e.data);
  }
  return `${ks}: ${data}`;
}

export interface MsgArg {
  subject: Uint8Array;
  reply?: Uint8Array;
  sid: number;
  hdr: number;
  size: number;
}

// Placeholder subject of a new MsgArg; it is only read in the MSG_PAYLOAD
// state, which processMsgArgs enters after setting the real subject. Being
// empty, it has nothing that could be modified, so it can be shared
const NO_SUBJECT = new Uint8Array(0);

function newMsgArg(): MsgArg {
  return {
    subject: NO_SUBJECT,
    reply: undefined,
    sid: -1,
    hdr: -1,
    size: -1,
  };
}

/**
 * The payload of a message that continues past the end of a read. Its size
 * is known from the control line, so it is allocated once and filled as the
 * reads arrive, rather than grown.
 */
export class SplitPayload {
  readonly bytes: Uint8Array;
  private received: number;

  constructor(head: Uint8Array, size: number) {
    // A read holds at most the whole payload, as a byte after it would have
    // dispatched it; if it held more, set would throw rather than drop bytes
    this.bytes = new Uint8Array(size);
    this.bytes.set(head);
    this.received = head.length;
  }

  /**
   * Returns the number of bytes of the payload that have arrived.
   */
  get length(): number {
    return this.received;
  }

  /**
   * Returns true once the whole payload has arrived.
   */
  isComplete(): boolean {
    return this.received === this.bytes.length;
  }

  /**
   * Copies as much of the rest of the payload as read holds from start on,
   * and returns the number of bytes copied.
   */
  fill(read: Uint8Array, start: number): number {
    const count = Math.min(
      this.bytes.length - this.received,
      read.length - start,
    );
    this.bytes.set(read.subarray(start, start + count), this.received);
    this.received += count;

    return count;
  }
}

// The control line at start began in a previous read: appends it to argBuf
// up to the next CR or NL in one write instead of byte by byte, and returns
// the index of the last byte appended; like the byte by byte copy did, it
// leaves a CR to the CR case, which drops it from the line
function appendLine(
  argBuf: DenoBuffer,
  buf: Uint8Array,
  start: number,
): number {
  const newline = buf.indexOf(cc.NL, start);
  const lineEnd = newline < 0 ? buf.length : newline;

  // Only search the line for a CR, not the payload after it
  const cr = buf.subarray(start, lineEnd).indexOf(cc.CR);
  const end = cr < 0 ? lineEnd : start + cr;

  argBuf.write(buf.subarray(start, end));
  return end - 1;
}

// Returns the index of the byte before the CR or NL that ends the control
// line at start, found with a native scan; if the line continues in the next
// read, its last byte may be the CR, so that byte is left to the state machine
function skipLine(buf: Uint8Array, start: number): number {
  const newline = buf.indexOf(cc.NL, start + 1);
  if (newline < 0) {
    return Math.max(start, buf.length - 2);
  }

  return (buf[newline - 1] === cc.CR ? newline - 1 : newline) - 1;
}

const ASCII_0 = 48;
const ASCII_9 = 57;
const MAX_64MB = 64 * 1024 * 1024;

// This is an almost verbatim port of the Go NATS parser
// https://github.com/nats-io/nats.go/blob/master/parser.go
// The longest control line, HMSG with a reply, has 5 arguments: subject,
// sid, reply, header size and total size
const MAX_TOKENS = 5;

// Plain numbers rather than cc.* so the per-byte checks compile to constants
const SPACE = 32;
const TAB = 9;
const CR = 13;
const NL = 10;

export class Parser {
  dispatcher: Dispatcher<ParserEvent>;
  state: State;
  as: number;
  drop: number;
  hdr: number;
  ma!: MsgArg;
  argBuf?: DenoBuffer;
  msgBuf?: SplitPayload;
  // Bounds [start, end) of each token of the last tokenized control line
  tokens: Int32Array = new Int32Array(MAX_TOKENS * 2);

  constructor(dispatcher: Dispatcher<ParserEvent>) {
    this.dispatcher = dispatcher;
    this.state = State.OP_START;
    this.as = 0;
    this.drop = 0;
    this.hdr = 0;
  }

  parse(buf: Uint8Array): void {
    let i: number;
    for (i = 0; i < buf.length; i++) {
      const b = buf[i];
      switch (this.state) {
        case State.OP_START:
          switch (b) {
            case cc.M:
            case cc.m:
              this.state = State.OP_M;
              this.hdr = -1;
              this.ma = newMsgArg();
              break;
            case cc.H:
            case cc.h:
              this.state = State.OP_H;
              this.hdr = 0;
              this.ma = newMsgArg();
              break;
            case cc.P:
            case cc.p:
              this.state = State.OP_P;
              break;
            case cc.PLUS:
              this.state = State.OP_PLUS;
              break;
            case cc.MINUS:
              this.state = State.OP_MINUS;
              break;
            case cc.I:
            case cc.i:
              this.state = State.OP_I;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_H:
          switch (b) {
            case cc.M:
            case cc.m:
              this.state = State.OP_M;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_M:
          switch (b) {
            case cc.S:
            case cc.s:
              this.state = State.OP_MS;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_MS:
          switch (b) {
            case cc.G:
            case cc.g:
              this.state = State.OP_MSG;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_MSG:
          switch (b) {
            case cc.SPACE:
            case cc.TAB:
              this.state = State.OP_MSG_SPC;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_MSG_SPC:
          switch (b) {
            case cc.SPACE:
            case cc.TAB:
              continue;
            default:
              this.state = State.MSG_ARG;
              this.as = i;
          }
          break;
        case State.MSG_ARG:
          switch (b) {
            case cc.CR:
              this.drop = 1;
              break;
            case cc.NL: {
              if (this.argBuf) {
                const arg = this.argBuf.bytes();
                this.processMsgArgs(arg, 0, arg.length);
              } else {
                this.processMsgArgs(buf, this.as, i - this.drop);
              }
              this.drop = 0;
              this.as = i + 1;
              this.state = State.MSG_PAYLOAD;

              // jump ahead with the index. If this overruns
              // what is left we fall out and process a split buffer.
              i = this.as + this.ma.size - 1;
              break;
            }
            default:
              // Move i to the byte before the CR or NL that ends the line, or
              // to the end of buf, so that the cases above handle it
              i = this.argBuf
                ? appendLine(this.argBuf, buf, i)
                : skipLine(buf, i);
          }
          break;
        case State.MSG_PAYLOAD:
          if (this.msgBuf) {
            if (this.msgBuf.isComplete()) {
              this.dispatcher.push(
                { kind: Kind.MSG, msg: this.ma, data: this.msgBuf.bytes },
              );
              this.argBuf = undefined;
              this.msgBuf = undefined;
              this.state = State.MSG_END;
            } else {
              // At least one byte is copied, as both the payload and the read
              // have some left
              i += this.msgBuf.fill(buf, i) - 1;
            }
          } else if (i - this.as >= this.ma.size) {
            this.dispatcher.push(
              { kind: Kind.MSG, msg: this.ma, data: buf.subarray(this.as, i) },
            );
            this.argBuf = undefined;
            this.msgBuf = undefined;
            this.state = State.MSG_END;
          }
          break;
        case State.MSG_END:
          switch (b) {
            case cc.NL:
              this.drop = 0;
              this.as = i + 1;
              this.state = State.OP_START;
              break;
            default:
              continue;
          }
          break;
        case State.OP_PLUS:
          switch (b) {
            case cc.O:
            case cc.o:
              this.state = State.OP_PLUS_O;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_PLUS_O:
          switch (b) {
            case cc.K:
            case cc.k:
              this.state = State.OP_PLUS_OK;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_PLUS_OK:
          switch (b) {
            case cc.NL:
              this.dispatcher.push({ kind: Kind.OK });
              this.drop = 0;
              this.state = State.OP_START;
              break;
          }
          break;
        case State.OP_MINUS:
          switch (b) {
            case cc.E:
            case cc.e:
              this.state = State.OP_MINUS_E;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_MINUS_E:
          switch (b) {
            case cc.R:
            case cc.r:
              this.state = State.OP_MINUS_ER;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_MINUS_ER:
          switch (b) {
            case cc.R:
            case cc.r:
              this.state = State.OP_MINUS_ERR;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_MINUS_ERR:
          switch (b) {
            case cc.SPACE:
            case cc.TAB:
              this.state = State.OP_MINUS_ERR_SPC;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_MINUS_ERR_SPC:
          switch (b) {
            case cc.SPACE:
            case cc.TAB:
              continue;
            default:
              this.state = State.MINUS_ERR_ARG;
              this.as = i;
          }
          break;
        case State.MINUS_ERR_ARG:
          switch (b) {
            case cc.CR:
              this.drop = 1;
              break;
            case cc.NL: {
              let arg: Uint8Array;
              if (this.argBuf) {
                arg = this.argBuf.bytes();
                this.argBuf = undefined;
              } else {
                arg = buf.subarray(this.as, i - this.drop);
              }
              this.dispatcher.push({ kind: Kind.ERR, data: arg });
              this.drop = 0;
              this.as = i + 1;
              this.state = State.OP_START;
              break;
            }
            default:
              if (this.argBuf) {
                this.argBuf.write(Uint8Array.of(b));
              }
          }
          break;
        case State.OP_P:
          switch (b) {
            case cc.I:
            case cc.i:
              this.state = State.OP_PI;
              break;
            case cc.O:
            case cc.o:
              this.state = State.OP_PO;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_PO:
          switch (b) {
            case cc.N:
            case cc.n:
              this.state = State.OP_PON;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_PON:
          switch (b) {
            case cc.G:
            case cc.g:
              this.state = State.OP_PONG;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_PONG:
          switch (b) {
            case cc.NL:
              this.dispatcher.push({ kind: Kind.PONG });
              this.drop = 0;
              this.state = State.OP_START;
              break;
          }
          break;
        case State.OP_PI:
          switch (b) {
            case cc.N:
            case cc.n:
              this.state = State.OP_PIN;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_PIN:
          switch (b) {
            case cc.G:
            case cc.g:
              this.state = State.OP_PING;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_PING:
          switch (b) {
            case cc.NL:
              this.dispatcher.push({ kind: Kind.PING });
              this.drop = 0;
              this.state = State.OP_START;
              break;
          }
          break;
        case State.OP_I:
          switch (b) {
            case cc.N:
            case cc.n:
              this.state = State.OP_IN;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_IN:
          switch (b) {
            case cc.F:
            case cc.f:
              this.state = State.OP_INF;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_INF:
          switch (b) {
            case cc.O:
            case cc.o:
              this.state = State.OP_INFO;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_INFO:
          switch (b) {
            case cc.SPACE:
            case cc.TAB:
              this.state = State.OP_INFO_SPC;
              break;
            default:
              throw this.fail(buf.subarray(i));
          }
          break;
        case State.OP_INFO_SPC:
          switch (b) {
            case cc.SPACE:
            case cc.TAB:
              continue;
            default:
              this.state = State.INFO_ARG;
              this.as = i;
          }
          break;
        case State.INFO_ARG:
          switch (b) {
            case cc.CR:
              this.drop = 1;
              break;
            case cc.NL: {
              let arg: Uint8Array;
              if (this.argBuf) {
                arg = this.argBuf.bytes();
                this.argBuf = undefined;
              } else {
                arg = buf.subarray(this.as, i - this.drop);
              }
              this.dispatcher.push({ kind: Kind.INFO, data: arg });
              this.drop = 0;
              this.as = i + 1;
              this.state = State.OP_START;
              break;
            }
            default:
              if (this.argBuf) {
                this.argBuf.writeByte(b);
              }
          }
          break;
        default:
          throw this.fail(buf.subarray(i));
      }
    }

    if (
      (this.state === State.MSG_ARG || this.state === State.MINUS_ERR_ARG ||
        this.state === State.INFO_ARG) && !this.argBuf
    ) {
      this.argBuf = new DenoBuffer(buf.subarray(this.as, i - this.drop));
    }

    if (this.state === State.MSG_PAYLOAD && !this.msgBuf) {
      // The payload continues in the next read; the subject and reply are left
      // as views of this read, like those of a message that fits in one read,
      // as dispatched messages reference a read's bytes and transports never
      // reuse its buffer
      this.msgBuf = new SplitPayload(buf.subarray(this.as), this.ma.size);
    }
  }

  // Splits line[pos, end) into at most MAX_TOKENS tokens, recording their
  // bounds in this.tokens as [start, end) pairs; returns the token count
  tokenize(line: Uint8Array, pos: number, end: number): number {
    const tokens = this.tokens;
    let count = 0;

    while (pos < end) {
      // Skip separators
      let byte = line[pos];
      if (
        byte <= SPACE &&
        (byte === SPACE || byte === TAB || byte === CR || byte === NL)
      ) {
        pos++;
        continue;
      }

      // Scan the token; printable bytes take a single compare
      const start = pos;
      for (pos++; pos < end; pos++) {
        byte = line[pos];
        if (
          byte <= SPACE &&
          (byte === SPACE || byte === TAB || byte === CR || byte === NL)
        ) {
          break;
        }
      }

      // More arguments than any control line has; report one too many so
      // the caller fails it as a parse error
      if (count === MAX_TOKENS) {
        return count + 1;
      }

      tokens[count * 2] = start;
      tokens[count * 2 + 1] = pos;
      count++;
    }

    return count;
  }

  token(line: Uint8Array, index: number): Uint8Array {
    return line.subarray(this.tokens[index * 2], this.tokens[index * 2 + 1]);
  }

  tokenInt(line: Uint8Array, index: number, max?: number): number {
    return this.parseIntRange(
      line,
      this.tokens[index * 2],
      this.tokens[index * 2 + 1],
      max,
    );
  }

  processMsgArgs(buf: Uint8Array, start: number, end: number): void {
    if (this.hdr >= 0) {
      return this.processHeaderMsgArgs(buf, start, end);
    }

    switch (this.tokenize(buf, start, end)) {
      case 3:
        this.ma.subject = this.token(buf, 0);
        this.ma.sid = this.tokenInt(buf, 1);
        this.ma.reply = undefined;
        this.ma.size = this.tokenInt(buf, 2, MAX_64MB);
        break;
      case 4:
        this.ma.subject = this.token(buf, 0);
        this.ma.sid = this.tokenInt(buf, 1);
        this.ma.reply = this.token(buf, 2);
        this.ma.size = this.tokenInt(buf, 3, MAX_64MB);
        break;
      default:
        throw this.argsError(buf, start, end, "Parse Error");
    }

    if (this.ma.sid < 0) {
      throw this.argsError(buf, start, end, "Bad or Missing Sid Error");
    }
    if (this.ma.size < 0) {
      throw this.argsError(buf, start, end, "Bad or Missing Size Error");
    }
  }

  fail(data: Uint8Array, label = ""): Error {
    if (!label) {
      label = `parse error [${this.state}]`;
    } else {
      label = `${label} [${this.state}]`;
    }

    return new Error(`${label}: ${TD.decode(data)}`);
  }

  processHeaderMsgArgs(buf: Uint8Array, start: number, end: number): void {
    switch (this.tokenize(buf, start, end)) {
      case 4:
        this.ma.subject = this.token(buf, 0);
        this.ma.sid = this.tokenInt(buf, 1);
        this.ma.reply = undefined;
        this.ma.hdr = this.tokenInt(buf, 2, MAX_64MB);
        this.ma.size = this.tokenInt(buf, 3, MAX_64MB);
        break;
      case 5:
        this.ma.subject = this.token(buf, 0);
        this.ma.sid = this.tokenInt(buf, 1);
        this.ma.reply = this.token(buf, 2);
        this.ma.hdr = this.tokenInt(buf, 3, MAX_64MB);
        this.ma.size = this.tokenInt(buf, 4, MAX_64MB);
        break;
      default:
        throw this.argsError(buf, start, end, "Parse Error");
    }

    if (this.ma.sid < 0) {
      throw this.argsError(buf, start, end, "Bad or Missing Sid Error");
    }
    if (this.ma.hdr < 0 || this.ma.hdr > this.ma.size) {
      throw this.argsError(buf, start, end, "Bad or Missing Header Size Error");
    }
    if (this.ma.size < 0) {
      throw this.argsError(buf, start, end, "Bad or Missing Size Error");
    }
  }

  // The error for the malformed MSG or HMSG arguments in buf[start, end); only
  // built on failure, so that a valid line takes no extra work
  argsError(
    buf: Uint8Array,
    start: number,
    end: number,
    problem: string,
  ): Error {
    const label = this.hdr >= 0 ? "processHeaderMsgArgs" : "processMsgArgs";
    return this.fail(buf.subarray(start, end), `${label} ${problem}`);
  }

  // Same as protoParseInt, over bytes[start, end)
  parseIntRange(
    bytes: Uint8Array,
    start: number,
    end: number,
    max?: number,
  ): number {
    // Max 999_999_999_999_999, one digit below MAX_SAFE_INTEGER
    const length = end - start;
    if (length <= 0 || length > 15) {
      return -1;
    }

    let value = 0;
    for (let pos = start; pos < end; pos++) {
      const byte = bytes[pos];
      if (byte < ASCII_0 || byte > ASCII_9) {
        return -1;
      }
      value = value * 10 + (byte - ASCII_0);
    }

    return max !== undefined && value > max ? -1 : value;
  }

  protoParseInt(a: Uint8Array, max?: number): number {
    return this.parseIntRange(a, 0, a.length, max);
  }
}

export const State = {
  OP_START: 0,
  OP_PLUS: 1,
  OP_PLUS_O: 2,
  OP_PLUS_OK: 3,
  OP_MINUS: 4,
  OP_MINUS_E: 5,
  OP_MINUS_ER: 6,
  OP_MINUS_ERR: 7,
  OP_MINUS_ERR_SPC: 8,
  MINUS_ERR_ARG: 9,
  OP_M: 10,
  OP_MS: 11,
  OP_MSG: 12,
  OP_MSG_SPC: 13,
  MSG_ARG: 14,
  MSG_PAYLOAD: 15,
  MSG_END: 16,
  OP_H: 17,
  OP_P: 18,
  OP_PI: 19,
  OP_PIN: 20,
  OP_PING: 21,
  OP_PO: 22,
  OP_PON: 23,
  OP_PONG: 24,
  OP_I: 25,
  OP_IN: 26,
  OP_INF: 27,
  OP_INFO: 28,
  OP_INFO_SPC: 29,
  INFO_ARG: 30,
} as const;

export type State = typeof State[keyof typeof State];

export const cc = {
  CR: "\r".charCodeAt(0),
  E: "E".charCodeAt(0),
  e: "e".charCodeAt(0),
  F: "F".charCodeAt(0),
  f: "f".charCodeAt(0),
  G: "G".charCodeAt(0),
  g: "g".charCodeAt(0),
  H: "H".charCodeAt(0),
  h: "h".charCodeAt(0),
  I: "I".charCodeAt(0),
  i: "i".charCodeAt(0),
  K: "K".charCodeAt(0),
  k: "k".charCodeAt(0),
  M: "M".charCodeAt(0),
  m: "m".charCodeAt(0),
  MINUS: "-".charCodeAt(0),
  N: "N".charCodeAt(0),
  n: "n".charCodeAt(0),
  NL: "\n".charCodeAt(0),
  O: "O".charCodeAt(0),
  o: "o".charCodeAt(0),
  P: "P".charCodeAt(0),
  p: "p".charCodeAt(0),
  PLUS: "+".charCodeAt(0),
  R: "R".charCodeAt(0),
  r: "r".charCodeAt(0),
  S: "S".charCodeAt(0),
  s: "s".charCodeAt(0),
  SPACE: " ".charCodeAt(0),
  TAB: "\t".charCodeAt(0),
} as const;

export type cc = typeof cc[keyof typeof cc];
