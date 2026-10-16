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

import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import {
  createInbox,
  deadline,
  deferred,
  delay,
  headers,
} from "../src/internal_mod.ts";
import type { Msg, NatsConnectionImpl } from "../src/internal_mod.ts";
import { cleanup, setup } from "nst";
import { errors } from "../src/errors.ts";

const te = new TextEncoder();

// Collects the next message published to subject; fails rather than hangs
// when a broken frame makes the server drop the connection
async function nextMsg(
  nc: Awaited<ReturnType<typeof setup>>["nc"],
  subject: string,
): Promise<{ received: Promise<Msg> }> {
  const received = deferred<Msg>();
  nc.subscribe(subject, {
    max: 1,
    callback: (_err, msg) => received.resolve(msg),
  });
  await nc.flush();
  return { received: deadline(received, 5000) };
}

Deno.test("publish - subject and reply that aren't strings are converted", async () => {
  const { ns, nc } = await setup();
  const { received } = await nextMsg(nc, "123");

  // possible from JavaScript; the frame used to be built with a template
  // string, which converted them
  // deno-lint-ignore no-explicit-any
  nc.publish(123 as any, "hi", { reply: 456 as any });

  const msg = await received;
  assertEquals(msg.subject, "123");
  assertEquals(msg.reply, "456");
  assertEquals(msg.string(), "hi");
  await cleanup(ns, nc);
});

Deno.test("publish - payload is captured when publish is called", async () => {
  const { ns, nc } = await setup();
  const subject = createInbox();
  const { received } = await nextMsg(nc, subject);

  const payload = te.encode("AAAA");
  nc.publish(subject, payload);
  // changing the array after publish doesn't change what is sent
  payload.fill(66);

  assertEquals((await received).string(), "AAAA");
  await cleanup(ns, nc);
});

Deno.test("publish - string payloads", async () => {
  const { ns, nc } = await setup();

  for (const text of ["", "ascii", "héllo wörld", "emoji 😀🚀", "日本語"]) {
    const subject = createInbox();
    const { received } = await nextMsg(nc, subject);
    nc.publish(subject, text);

    const msg = await received;
    assertEquals(msg.string(), text);
    assertEquals(msg.data.length, te.encode(text).length);
  }

  // with headers too
  const subject = createInbox();
  const { received } = await nextMsg(nc, subject);
  const h = headers();
  h.set("k", "v");
  nc.publish(subject, "héllo", { headers: h });

  const msg = await received;
  assertEquals(msg.string(), "héllo");
  assertEquals(msg.headers?.get("k"), "v");
  await cleanup(ns, nc);
});

Deno.test("publish - only written messages are counted", async () => {
  const { ns, nc } = await setup({ max_payload: 1024 });
  const before = nc.stats();

  nc.publish("a", new Uint8Array(10));
  assertThrows(
    () => nc.publish("a", new Uint8Array(2048)),
    errors.InvalidArgumentError,
    "max_payload",
  );

  const after = nc.stats();
  assertEquals(after.outMsgs - before.outMsgs, 1);
  assertEquals(after.outBytes - before.outBytes, 10);

  // the failed publish left nothing behind that breaks later messages
  const subject = createInbox();
  const { received } = await nextMsg(nc, subject);
  nc.publish(subject, "ok");
  assertEquals((await received).string(), "ok");
  await cleanup(ns, nc);
});

Deno.test("request - returns a promise callers can't settle", async () => {
  const { ns, nc } = await setup();
  const subject = createInbox();
  nc.subscribe(subject, {
    callback: (_err, msg) => {
      msg.respond("pong");
    },
  });
  await nc.flush();

  const p = nc.request(subject, "ping");
  assert(!("resolve" in p));
  assert(!("reject" in p));
  assertEquals((await p).string(), "pong");
  await cleanup(ns, nc);
});

Deno.test("request - times out with a TimeoutError", async () => {
  const { ns, nc } = await setup();
  const subject = createInbox();
  // a subscriber that never answers, so the server has a responder
  nc.subscribe(subject, { callback: () => {} });
  await nc.flush();

  const start = Date.now();
  await assertRejects(
    () => nc.request(subject, "ping", { timeout: 250 }),
    errors.TimeoutError,
  );
  assert(Date.now() - start >= 200);

  // the timed out request is no longer waiting for a reply
  const impl = nc as NatsConnectionImpl;
  assertEquals(impl.protocol.muxSubscriptions.size(), 0);
  await cleanup(ns, nc);
});

Deno.test("request - a request nobody waits for doesn't reject unhandled", async () => {
  const { ns, nc } = await setup();
  const subject = createInbox();
  nc.subscribe(subject, { callback: () => {} });
  await nc.flush();

  let unhandled = 0;
  const count = (e: PromiseRejectionEvent) => {
    unhandled++;
    e.preventDefault();
  };
  globalThis.addEventListener("unhandledrejection", count);
  try {
    // neither awaited nor given a handler; it times out
    nc.request(subject, "ping", { timeout: 100 });
    await delay(300);
  } finally {
    globalThis.removeEventListener("unhandledrejection", count);
  }

  assertEquals(unhandled, 0);
  await cleanup(ns, nc);
});
