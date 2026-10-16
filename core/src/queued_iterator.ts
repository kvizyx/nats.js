/*
 * Copyright 2020-2022 The NATS Authors
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
import type { Deferred } from "./util.ts";
import { deferred } from "./util.ts";
import type { QueuedIterator } from "./core.ts";
import type { CallbackFn, Dispatcher } from "./core.ts";
import { InvalidOperationError } from "./errors.ts";

export class QueuedIteratorImpl<T> implements QueuedIterator<T>, Dispatcher<T> {
  inflight: number;
  processed: number;
  // this is updated by the protocol
  received: number;
  noIterator: boolean;
  iterClosed: Deferred<void | Error>;
  done: boolean;
  signal: Deferred<void>;
  yields: (T | CallbackFn)[];
  filtered: number;
  pendingFiltered: number;
  ctx?: unknown;
  _data?: unknown; //data is for use by extenders in any way they like
  err?: Error;
  time: number;
  profile: boolean;
  yielding: boolean;
  didBreak: boolean;

  constructor() {
    this.inflight = 0;
    this.filtered = 0;
    this.pendingFiltered = 0;
    this.processed = 0;
    this.received = 0;
    this.noIterator = false;
    this.done = false;
    this.signal = deferred<void>();
    this.yields = [];
    this.iterClosed = deferred<void | Error>();
    this.time = 0;
    this.yielding = false;
    this.didBreak = false;
    this.profile = false;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.iterate();
  }

  push(v: T | CallbackFn): void {
    if (this.done) {
      return;
    }
    // if they `break` from a `for await`, any signaling that is pushed via
    // a function is not handled this can prevent closed promises from
    // resolving downstream.
    if (this.didBreak) {
      if (typeof v === "function") {
        const cb = v as CallbackFn;
        try {
          cb();
        } catch (_) {
          // ignored
        }
      }
      return;
    }
    if (typeof v === "function") {
      this.pendingFiltered++;
    }
    this.yields.push(v);
    this.signal.resolve();
  }

  iterate(): AsyncIterableIterator<T> {
    return new QueuedIteratorIter<T>(this);
  }

  stop(err?: Error): void {
    if (this.done) {
      return;
    }
    this.err = err;
    this.done = true;
    this.signal.resolve();
    this.iterClosed.resolve(err);
  }

  getProcessed(): number {
    return this.noIterator ? this.received : this.processed;
  }

  getPending(): number {
    return this.yields.length + this.inflight - this.pendingFiltered;
  }

  getReceived(): number {
    return this.received - this.filtered;
  }
}

// A new result each time, like a generator returns, so that a caller that
// changes a result doesn't affect other calls
function doneResult<T>(): IteratorResult<T> {
  return { value: undefined, done: true };
}

// The prototype that async generators inherit from; the iterator below
// inherits from it too, so that it has what they have beyond next, return and
// throw, such as Symbol.asyncDispose for `await using`
const AsyncIteratorPrototype = Object.getPrototypeOf(
  Object.getPrototypeOf(async function* () {}).prototype,
);

/**
 * Iterates a QueuedIteratorImpl. Hand-written rather than an async generator:
 * an item that is already queued resolves without the generator machinery,
 * which roughly halves the per-message cost.
 *
 * It behaves like the generator it replaces: the checks run on the first
 * next(), calls settle in the order they were made, and finishing (end,
 * error, break/return) marks the queue as broken and stops it.
 */
class QueuedIteratorIter<T> implements AsyncIterableIterator<T> {
  queue: QueuedIteratorImpl<T>;

  // Batch being yielded; null when a new one must be taken from the queue
  batch: (T | CallbackFn)[] | null;
  index: number;

  started: boolean;
  finished: boolean;

  // Set once the signal was awaited for an empty queue
  signaled: boolean;

  // Set after an item is yielded, so the next call records its time
  yielded: boolean;
  start: number;

  // The last next() that has not settled; later calls queue behind it
  queued: Promise<IteratorResult<T>> | null;

  constructor(queue: QueuedIteratorImpl<T>) {
    this.queue = queue;
    this.batch = null;
    this.index = 0;
    this.started = false;
    this.finished = false;
    this.signaled = false;
    this.yielded = false;
    this.start = 0;
    this.queued = null;
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<T> {
    return this;
  }

  next(): Promise<IteratorResult<T>> {
    if (this.queued) {
      // Like a generator, calls settle in order; a call after a failed one
      // sees the end
      const advanceQueued = () => this.advance(false);
      return this.track(this.queued.then(advanceQueued, advanceQueued));
    }

    return this.advance(true);
  }

  // The track flag is false for calls already queued, so they don't
  // replace the tail
  advance(track: boolean): Promise<IteratorResult<T>> {
    let result: IteratorResult<T> | null;
    try {
      result = this.step();
    } catch (err) {
      return Promise.reject(err);
    }

    // The common case: an item was already queued
    if (result) {
      return Promise.resolve(result);
    }

    // The queue is empty; wait for the next push or stop
    const waiting = this.queue.signal.then(() => {
      this.signaled = true;
      return this.advance(false);
    });
    return track ? this.track(waiting) : waiting;
  }

  // Makes call the last unsettled call; later calls queue behind it
  track(call: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
    this.queued = call;

    const clear = () => {
      if (this.queued === call) {
        this.queued = null;
      }
    };
    call.then(clear, clear);

    return call;
  }

  return(): Promise<IteratorResult<T>> {
    const done = (): IteratorResult<T> => {
      this.finish();
      return doneResult();
    };

    // Like a generator, a return() waits for the calls before it
    return this.queued
      ? this.track(this.queued.then(done, done))
      : Promise.resolve(done());
  }

  throw(err?: unknown): Promise<IteratorResult<T>> {
    const fail = (): Promise<IteratorResult<T>> => {
      this.finish();
      return Promise.reject(err);
    };

    return this.queued ? this.track(this.queued.then(fail, fail)) : fail();
  }

  finish(): void {
    if (this.finished) {
      return;
    }
    this.finished = true;
    this.batch = null;

    // A generator that never started doesn't run its finally
    if (this.started) {
      this.queue.didBreak = true;
      this.queue.stop();
    }
  }

  // Returns the next result, or null if the queue is empty and the signal
  // must be awaited; throws, after finishing, if the iterator failed
  step(): IteratorResult<T> | null {
    if (this.finished) {
      return doneResult();
    }

    const queue = this.queue;
    if (!this.started) {
      // Like the generator's prologue, these don't stop the queue
      if (queue.noIterator) {
        this.finished = true;
        throw new InvalidOperationError(
          "iterator cannot be used when a callback is registered",
        );
      }
      if (queue.yielding) {
        this.finished = true;
        throw new InvalidOperationError("iterator is already yielding");
      }

      queue.yielding = true;
      this.started = true;
    }

    // The time the loop body took with the previous item
    if (this.yielded) {
      this.yielded = false;
      queue.time = queue.profile ? Date.now() - this.start : 0;
    }

    try {
      while (true) {
        let batch = this.batch;

        // Take everything queued so far as the next batch
        if (batch === null) {
          if (queue.yields.length === 0 && !this.signaled) {
            return null;
          }
          this.signaled = false;

          if (queue.err) {
            throw queue.err;
          }

          batch = this.batch = queue.yields;
          this.index = 0;
          queue.inflight = batch.length;
          queue.yields = [];
        }

        while (this.index < batch.length) {
          const item = batch[this.index++];

          // Queued functions are internal callbacks, not items
          if (typeof item === "function") {
            queue.pendingFiltered--;
            (item as CallbackFn)();

            // The callback could have also set an error
            if (queue.err) {
              throw queue.err;
            }
            continue;
          }

          queue.processed++;
          queue.inflight--;
          this.start = queue.profile ? Date.now() : 0;
          this.yielded = true;
          return { value: item as T, done: false };
        }

        this.batch = null;

        // Yielding could have paused and a microtask could have added
        // messages; reuse the batch array to prevent allocations if possible
        if (queue.done) {
          this.finish();
          return doneResult();
        } else if (queue.yields.length === 0) {
          batch.length = 0;
          queue.yields = batch;
          queue.signal = deferred();
        }
      }
    } catch (err) {
      this.finish();
      throw err;
    }
  }
}

Object.setPrototypeOf(QueuedIteratorIter.prototype, AsyncIteratorPrototype);
