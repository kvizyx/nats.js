/*
 * Copyright 2024 Synadia Communications, Inc
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

import { assertEquals } from "@std/assert";

import { decode, Empty, encode, utf8Length } from "../src/encoders.ts";

Deno.test("encoders - basics", () => {
  let buf = encode();
  assertEquals(buf, Empty);

  buf = encode("1", "2", "3", "4");
  assertEquals(buf, Uint8Array.from([49, 50, 51, 52]));

  const s = decode(buf);
  assertEquals(s, "1234");

  assertEquals(decode(Empty), "");
});

Deno.test("encoders - utf8Length matches TextEncoder", () => {
  const te = new TextEncoder();
  const samples = [
    "",
    "ascii",
    "\u007f\u0080",
    "\u07ff\u0800",
    "héllo wörld",
    "日本語",
    "emoji 😀🚀",
    // lone and reversed surrogates encode as U+FFFD
    "\ud800",
    "\udc00",
    "a\ud800b",
    "\udc00\ud800",
    "\ud83d",
    "x\ud83d",
  ];

  // every code unit, alone and next to its neighbour
  for (let code = 0; code <= 0xffff; code += 7) {
    samples.push(String.fromCharCode(code, code + 1));
  }

  for (const text of samples) {
    assertEquals(
      utf8Length(text),
      te.encode(text).length,
      JSON.stringify(text),
    );
  }
});
