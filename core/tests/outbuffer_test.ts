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

import { assert, assertEquals, assertThrows } from "@std/assert";
import { formatSafeInt, OutBuffer } from "../src/outbuffer.ts";

const te = new TextEncoder();
const td = new TextDecoder();

// The frame the old code built with a template string
function expectedFrame(
  subject: string,
  reply: string,
  headers: Uint8Array | null,
  data: Uint8Array,
): Uint8Array {
  const headersLen = headers ? headers.length : 0;
  const command = headers ? "HPUB" : "PUB";
  const args = [subject];
  if (reply) {
    args.push(reply);
  }
  if (headers) {
    args.push(`${headersLen}`);
  }
  args.push(`${headersLen + data.length}`);

  const line = te.encode(`${command} ${args.join(" ")}\r\n`);
  const frame = new Uint8Array(
    line.length + headersLen + data.length + 2,
  );
  frame.set(line);
  if (headers) {
    frame.set(headers, line.length);
  }

  frame.set(data, line.length + headersLen);
  frame.set(te.encode("\r\n"), line.length + headersLen + data.length);
  return frame;
}

// Compares without assertEquals, whose diff of large arrays runs out of memory
function assertSameBytes(actual: Uint8Array, expected: Uint8Array): void {
  assertEquals(actual.length, expected.length);
  const mismatch = actual.findIndex((byte, index) => byte !== expected[index]);
  assertEquals(mismatch, -1, `bytes differ at ${mismatch}`);
}

function writeBytes(
  ob: OutBuffer,
  subject: string,
  reply: string,
  headers: Uint8Array | null,
  data: Uint8Array,
): void {
  ob.writeMsg(subject, reply, headers, data);
}

Deno.test("outbuffer - pub and hpub frames", () => {
  const headers = te.encode("NATS/1.0\r\nk: v\r\n\r\n");
  const data = te.encode("hello");
  const cases: [string, string, Uint8Array | null, Uint8Array][] = [
    ["a", "", null, data],
    ["a.b", "_INBOX.x", null, data],
    ["a", "", headers, data],
    ["a.b", "_INBOX.x", headers, data],
    ["empty", "", null, new Uint8Array(0)],
    ["empty", "r", headers, new Uint8Array(0)],
  ];

  for (const [subject, reply, hdrs, payload] of cases) {
    const ob = new OutBuffer();
    writeBytes(ob, subject, reply, hdrs, payload);
    assertEquals(ob.drain(), expectedFrame(subject, reply, hdrs, payload));
  }
});

Deno.test("outbuffer - non-ascii subjects", () => {
  const data = te.encode("x");
  for (const subject of ["ünï.cödé", "emoji.😀.🚀", "é".repeat(500)]) {
    const ob = new OutBuffer();
    writeBytes(ob, subject, "réply", null, data);
    assertEquals(ob.drain(), expectedFrame(subject, "réply", null, data));
  }
});

Deno.test("outbuffer - a write that throws leaves no partial frame", () => {
  const ob = new OutBuffer();
  writeBytes(ob, "before", "", null, te.encode("1"));
  const before = ob.size();

  // a subject that fails after "PUB " is written
  const subject = {
    length: 4,
    charCodeAt() {
      throw new Error("boom");
    },
  } as unknown as string;
  assertThrows(
    () => writeBytes(ob, subject, "", null, te.encode("x")),
    Error,
    "boom",
  );
  assertEquals(ob.size(), before);

  writeBytes(ob, "after", "", null, te.encode("2"));
  assertEquals(
    td.decode(ob.drain()),
    "PUB before 1\r\n1\r\nPUB after 1\r\n2\r\n",
  );
});

Deno.test("outbuffer - only safe integers are written as sizes", () => {
  for (const v of [-1, 1.5, NaN, Infinity, 2 ** 53, 2 ** 60]) {
    assertThrows(() => formatSafeInt(v), RangeError);
  }

  for (const v of [0, 9, 10, 2 ** 31, Number.MAX_SAFE_INTEGER]) {
    assertEquals(formatSafeInt(v), `${v}`);
  }
});

Deno.test("outbuffer - fill writes commands as they are", () => {
  const ob = new OutBuffer();
  ob.fill("SUB a 1\r\n");
  ob.fill(te.encode("PING\r\n"));
  ob.fill(te.encode("CMD\r\n"), te.encode("payload"), te.encode("\r\n"));
  assertEquals(
    td.decode(ob.drain()),
    "SUB a 1\r\nPING\r\nCMD\r\npayload\r\n",
  );
});

Deno.test("outbuffer - drained frames are not changed by later writes", () => {
  const ob = new OutBuffer();

  // small frames are copied out of the reused buffer
  writeBytes(ob, "a", "", null, te.encode("first"));
  const small = ob.drain();
  writeBytes(ob, "b", "", null, te.encode("other"));
  assertEquals(td.decode(small), "PUB a 5\r\nfirst\r\n");
  ob.drain();

  // two large frames fill the buffer, which is handed off itself: the frame
  // is a view of it rather than an exact copy
  const big = new Uint8Array(200_000).fill(1);
  writeBytes(ob, "c", "", null, big);
  writeBytes(ob, "c", "", null, big);
  const large = ob.drain();
  assert(large.buffer.byteLength > large.length);
  assert(large.buffer.byteLength < large.length * 1.1);
  writeBytes(ob, "d", "", null, new Uint8Array(200_000).fill(2));
  writeBytes(ob, "d", "", null, new Uint8Array(200_000).fill(2));
  const frame = expectedFrame("c", "", null, big);
  const both = new Uint8Array(frame.length * 2);
  both.set(frame);
  both.set(frame, frame.length);
  assertSameBytes(large, both);
});

Deno.test("outbuffer - frames of many sizes", () => {
  const sizes = [0, 1, 15, 16, 17, 1000, 65_535, 65_536, 65_537, 300_000];
  const ob = new OutBuffer();
  const expected: Uint8Array[] = [];
  const drained: Uint8Array[] = [];

  let fill = 0;
  for (let round = 0; round < 3; round++) {
    for (const size of sizes) {
      const data = new Uint8Array(size).fill(fill++ % 256);
      writeBytes(ob, `s.${size}`, round ? "r" : "", null, data);
      expected.push(expectedFrame(`s.${size}`, round ? "r" : "", null, data));
    }

    drained.push(ob.drain());
  }

  const join = (parts: Uint8Array[]) => {
    const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
    let pos = 0;
    for (const part of parts) {
      out.set(part, pos);
      pos += part.length;
    }
    return out;
  };
  assertSameBytes(join(drained), join(expected));
  assertEquals(ob.size(), 0);
});

Deno.test("outbuffer - a large frame doesn't hold an unused buffer", () => {
  const big = new Uint8Array(100 * 1024).fill(7);
  const small = new Uint8Array(1024);

  // a single large frame, and one after small frames that didn't reach a
  // flush; the room a grow leaves for another frame is never filled
  const cases: Uint8Array[][] = [[big], [...Array(30).fill(small), big]];
  for (const payloads of cases) {
    const ob = new OutBuffer();
    for (const payload of payloads) {
      writeBytes(ob, "s", "", null, payload);
    }

    const frame = ob.drain();
    // the transport holds the frame's whole backing buffer until it is
    // written
    assert(frame.buffer.byteLength < 2 * frame.length);
    assertSameBytes(
      frame.subarray(frame.length - big.length - 2),
      expectedFrame("s", "", null, big).subarray(-big.length - 2),
    );
  }
});

Deno.test("outbuffer - a huge frame doesn't hold twice its size", () => {
  const ob = new OutBuffer();
  const size = 8 * 1024 * 1024;
  writeBytes(ob, "big", "", null, new Uint8Array(size));

  const frame = ob.drain();
  // the transport holds the frame's whole backing buffer until it is
  // written, so that buffer must not be much larger than the frame
  assert(frame.buffer.byteLength < size + 2 * 1024 * 1024);
});
