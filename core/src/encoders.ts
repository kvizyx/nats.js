/*
 * Copyright 2020 The NATS Authors
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
export const Empty: Uint8Array = new Uint8Array(0);

export const TE: InstanceType<typeof TextEncoder> = new TextEncoder();
export const TD: InstanceType<typeof TextDecoder> = new TextDecoder();

function concat(...bufs: Uint8Array[]): Uint8Array {
  let max = 0;
  for (let i = 0; i < bufs.length; i++) {
    max += bufs[i].length;
  }
  const out = new Uint8Array(max);
  let index = 0;
  for (let i = 0; i < bufs.length; i++) {
    out.set(bufs[i], index);
    index += bufs[i].length;
  }
  return out;
}

export function encode(...a: string[]): Uint8Array {
  const bufs = [];
  for (let i = 0; i < a.length; i++) {
    bufs.push(TE.encode(a[i]));
  }
  if (bufs.length === 0) {
    return Empty;
  }
  if (bufs.length === 1) {
    return bufs[0];
  }
  return concat(...bufs);
}

// UTF-8 sizes by UTF-16 code unit
const MAX_ONE_BYTE = 0x7f;
const MAX_TWO_BYTES = 0x7ff;
const HIGH_SURROGATE_FIRST = 0xd800;
const HIGH_SURROGATE_LAST = 0xdbff;
const LOW_SURROGATE_FIRST = 0xdc00;
const LOW_SURROGATE_LAST = 0xdfff;

/**
 * Returns the number of bytes TextEncoder produces for text, without
 * encoding it. A lone surrogate is encoded as U+FFFD, which takes 3 bytes.
 */
export function utf8Length(text: string): number {
  let bytes = 0;

  for (let index = 0; index < text.length; index++) {
    const code = text.charCodeAt(index);
    if (code <= MAX_ONE_BYTE) {
      bytes += 1;
    } else if (code <= MAX_TWO_BYTES) {
      bytes += 2;
    } else if (
      code >= HIGH_SURROGATE_FIRST && code <= HIGH_SURROGATE_LAST &&
      index + 1 < text.length
    ) {
      const next = text.charCodeAt(index + 1);
      if (next >= LOW_SURROGATE_FIRST && next <= LOW_SURROGATE_LAST) {
        // A surrogate pair is one code point of 4 bytes
        bytes += 4;
        index++;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
  }

  return bytes;
}

export function decode(a: Uint8Array): string {
  if (!a || a.length === 0) {
    return "";
  }
  return TD.decode(a);
}
