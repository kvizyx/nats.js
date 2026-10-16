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
import { TE } from "./encoders.ts";

const INITIAL_SIZE = 64 * 1024;
const NONE = new Uint8Array(0);

// A UTF-16 code unit encodes to at most 3 UTF-8 bytes; a surrogate pair is
// 2 units and 4 bytes
const MAX_UTF8_PER_UNIT = 3;
// The last ASCII code; characters up to it encode to a single UTF-8 byte
// equal to their code
const MAX_ASCII = 0x7f;
const ASCII_0 = 48;

// Pieces of the PUB and HPUB control lines
const PUB = TE.encode("PUB");
const HPUB = TE.encode("HPUB");
const CRLF = TE.encode("\r\n");
const SPACE = 0x20;
// More than a control line takes besides its subject and reply: the command,
// separators, sizes and CRLF
const CONTROL_LINE_HINT = 64;
// Arrays up to this size are copied with a loop rather than Uint8Array.set
const SMALL_COPY = 16;
// The most room a grow leaves for another frame of the same size
const MAX_FRAME_HEADROOM = 1024 * 1024;

/**
 * A single growable outbound buffer that protocol frames are written into
 * directly, with no per-publish strings, TextEncoder allocations or chunk
 * arrays.
 *
 * Writes past the end of a Uint8Array are silently dropped and
 * TextEncoder.encodeInto silently truncates, so space is never assumed:
 * every write reserves the space it needs itself.
 */
export class OutBuffer {
  private buf = new Uint8Array(INITIAL_SIZE);
  private len = 0;

  /**
   * Writes a message frame, PUB without headers and HPUB with them:
   * - `PUB <subject> [reply] <size>\r\n<payload>\r\n`
   * - `HPUB <subject> [reply] <hsize> <size>\r\n<headers><payload>\r\n`.
   */
  writeMsg(
    subject: string,
    reply: string,
    headers: Uint8Array | null,
    data: Uint8Array,
  ): void {
    this.writeFrame(subject, reply, headers, data, data.length);
  }

  /**
   * Like writeMsg, for a string payload, which is encoded straight into the
   * buffer. textLength is its size in bytes, utf8Length(text); the caller
   * needs it before writing to check the payload limit. If it doesn't match
   * the encoding, the write throws and leaves the buffer as it was.
   */
  writeTextMsg(
    subject: string,
    reply: string,
    headers: Uint8Array | null,
    text: string,
    textLength: number,
  ): void {
    this.writeFrame(subject, reply, headers, text, textLength);
  }

  /**
   * Writes a protocol command as it is, such as SUB, UNSUB or PING, and any
   * payloads after it.
   */
  fill(command: Uint8Array | string, ...payloads: Uint8Array[]): void {
    // Like a message frame, never leave a partial command behind
    const start = this.len;
    try {
      if (typeof command === "string") {
        this.writeString(command);
      } else if (command) {
        this.writeBytes(command);
      }

      for (const payload of payloads) {
        if (payload) {
          this.writeBytes(payload);
        }
      }
    } catch (err) {
      this.len = start;
      throw err;
    }
  }

  /**
   * Returns the pending bytes as a buffer the caller owns. Up to the initial
   * size they are copied so the internal buffer can be reused. Larger ones
   * hand off the internal buffer without a copy, and the next write allocates
   * a new one, which also releases memory after a burst of large messages.
   * The returned view may not span its whole ArrayBuffer.
   */
  drain(): Uint8Array {
    const pending = this.len;
    this.len = 0;

    if (pending > INITIAL_SIZE) {
      const frame = this.buf.subarray(0, pending);
      this.buf = NONE;
      return frame;
    }

    return this.buf.slice(0, pending);
  }

  /**
   * Returns the number of pending bytes.
   */
  size(): number {
    return this.len;
  }

  /**
   * Drops the pending bytes and releases a buffer that grew past the initial
   * size.
   */
  reset(): void {
    this.len = 0;

    if (this.buf.length > INITIAL_SIZE) {
      this.buf = NONE;
    }
  }

  private writeFrame(
    subject: string,
    reply: string,
    headers: Uint8Array | null,
    payload: Uint8Array | string,
    payloadLength: number,
  ): void {
    const headersLen = headers ? headers.length : 0;
    const size = headersLen + payloadLength;

    // Only a hint so that the buffer grows once for the whole frame and its
    // headroom fits the next frame of the same size; every write below still
    // reserves its own space
    this.ensure(CONTROL_LINE_HINT + subject.length + reply.length + size);

    // A frame is written in pieces; if any of them throws, drop what was
    // written so that the buffer never holds a partial frame
    const start = this.len;
    try {
      this.writeBytes(headers ? HPUB : PUB);
      this.writeByte(SPACE);
      this.writeString(subject);

      if (reply) {
        this.writeByte(SPACE);
        this.writeString(reply);
      }

      if (headers) {
        this.writeByte(SPACE);
        this.writeSafeInt(headersLen);
      }

      this.writeByte(SPACE);
      this.writeSafeInt(size);
      this.writeBytes(CRLF);

      if (headers) {
        this.writeBytes(headers);
      }

      if (typeof payload === "string") {
        this.writeEncoded(payload, payloadLength);
      } else {
        this.writeBytes(payload);
      }

      this.writeBytes(CRLF);
    } catch (err) {
      this.len = start;
      throw err;
    }
  }

  // Makes room for count more bytes; only the check lives here so that it is
  // cheap to call from every writer
  private ensure(count: number): void {
    if (this.len + count > this.buf.length) {
      this.grow(count);
    }
  }

  private grow(count: number): void {
    // Doubling what is buffered keeps appends cheap overall, and room for one
    // more frame of this size lets two large frames share a buffer without
    // another grow and copy; that room is capped so that a single huge frame
    // doesn't take twice its size
    const grown = new Uint8Array(
      Math.max(
        INITIAL_SIZE,
        2 * this.len + count + Math.min(count, MAX_FRAME_HEADROOM),
      ),
    );

    if (this.len) {
      grown.set(this.buf.subarray(0, this.len));
    }

    this.buf = grown;
  }

  private writeByte(byte: number): void {
    this.ensure(1);
    this.buf[this.len++] = byte;
  }

  private writeBytes(bytes: Uint8Array): void {
    if (bytes.length === 0) {
      return;
    }

    this.ensure(bytes.length);

    // Uint8Array.set has a fixed cost that dominates for a few bytes
    if (bytes.length <= SMALL_COPY) {
      const out = this.buf;
      for (let index = 0; index < bytes.length; index++) {
        out[this.len++] = bytes[index];
      }
      return;
    }

    this.buf.set(bytes, this.len);
    this.len += bytes.length;
  }

  private writeString(text: string): void {
    this.ensure(text.length * MAX_UTF8_PER_UNIT);

    // ASCII is written directly; the rest is left to the encoder
    const out = this.buf;
    let pos = this.len;

    for (let index = 0; index < text.length; index++) {
      const code = text.charCodeAt(index);
      if (code > MAX_ASCII) {
        pos += TE.encodeInto(text.substring(index), out.subarray(pos)).written;
        break;
      }

      out[pos++] = code;
    }

    this.len = pos;
  }

  // Encodes text into exactly byteLength bytes; throws if its encoding has a
  // different size, so a wrong length can't truncate or misplace a payload
  private writeEncoded(text: string, byteLength: number): void {
    this.ensure(byteLength);

    const target = this.buf.subarray(this.len, this.len + byteLength);
    const { read, written } = TE.encodeInto(text, target);
    if (read !== text.length || written !== byteLength) {
      throw new RangeError(
        `payload encodes to a different size than ${byteLength} bytes`,
      );
    }

    this.len += written;
  }

  // Writes value as decimal digits
  private writeSafeInt(value: number): void {
    const digits = safeIntDigits(value);
    this.ensure(digits);

    // Write the digits backwards from the last one; (value - digit) / 10 is
    // exact integer division and, unlike | 0, also works past 2^31
    const out = this.buf;
    let pos = this.len + digits;
    this.len = pos;

    do {
      const digit = value % 10;
      out[--pos] = ASCII_0 + digit;
      value = (value - digit) / 10;
    } while (value > 0);
  }
}

/**
 * Returns the number of decimal digits of value. Only non-negative safe
 * integers are written exactly, so any other value throws.
 */
export function safeIntDigits(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`expected a non-negative safe integer, got ${value}`);
  }

  let digits = 1;
  for (let power = 10; power <= value; power *= 10) {
    digits++;
  }

  return digits;
}
