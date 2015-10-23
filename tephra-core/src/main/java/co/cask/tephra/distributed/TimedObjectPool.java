/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tephra.distributed;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * A TimedObjectPool is an object pool that can dynamically shrink and grow.
 * Objects in the pool will be destroyed and discarded based upon a configured
 * timeout. For instance, if the time out is one minute, and an object hasn't
 * been used for two minutes, then the object will be cleaned up the next time
 * that the cleanup happens.
 *
 * Normally, an element is obtained by a client and then returned to the pool
 * after use. However, if the element gets into a bad state, the client can
 * also discard the element. This will cause the element to be removed from
 * the pool, and for a subsequent request, a new element can be created
 * on the fly to replace the discarded one.
 *
 * The pool starts with zero (active) elements. Every time a client attempts
 * to obtain an element, an element from the pool is returned if available.
 * Otherwise, a new element is created (using abstract method create(), this
 * must be overridden by all implementations), and the number of active elements
 * is increased by one.
 *
 * Every time an element is discarded, it is "destroyed" in order to properly
 * release all of its resources before discarding. Every time an element is
 * returned to the pool, it is "recycled" to restore its fresh state for the
 * next use.
 *
 * @param <T> the type of the elements
 * @param <E> the type of exception thrown by create()
 */
public abstract class TimedObjectPool<T, E extends Exception> {

  /**
   * A method to create a new element. Will be called every time {@link #obtain}
   * is called and the pool of available elements is empty.
   *
   * @return a new element
   */
  protected abstract T create() throws E;

  /**
   * A method to destroy an element. This gets called every time an element
   * is discarded, to properly release all resources that the element might
   * hold.
   *
   * @param element the element to destroy
   */
  protected void destroy(T element) {
    // by default do nothing
  }

  /**
   * A method to recycle an existing element when it is returned to the pool.
   * This method ensures that the element is in a fresh state before it can
   * be reused by the next agent.
   *
   * @param element the element to recycle
   */
  protected void recycle(T element) {
    // by default do nothing
  }


  private final class TimestampedEntry {
    private final T element;
    private final long timestamp;

    private TimestampedEntry(T element, long timestamp) {
      this.element = element;
      this.timestamp = timestamp;
    }

    private T getElement() {
      return element;
    }

    private long getTimestamp() {
      return timestamp;
    }
  }

  // holds all currently available elements, with the elements at the head
  // being the most recently accessed
  private final Deque<TimestampedEntry> elements;

  // timeout, in milliseconds, after which an element can be destroyed and removed
  private final long timeoutMillis;

  private final AtomicBoolean isCleanupInProgress = new AtomicBoolean(false);

  public TimedObjectPool(long timeoutMillis) {
    this.elements = new ConcurrentLinkedDeque<>();
    this.timeoutMillis = timeoutMillis;
  }

  /**
   * Get a element from the pool. If there is an available element in
   * the pool, it will be returned. Otherwise, a new element is created with
   * create() and returned.
   * Note that housekeeping is done within this method.
   *
   * @return an element
   */
  public T obtain() throws E {
    T element = getOrCreate();
    doCleanup();
    return element;
  }

  /**
   * Goes through all elements and destroys and removes the elements that have
   * not been used for a time period greater than or equal to the configured timeout.
   */
  private void doCleanup() {
    // Only one thread needs to do cleanup at any given time.
    if (!isCleanupInProgress.compareAndSet(false, true)) {
      return;
    }

    try {
      long cleanupTime = System.currentTimeMillis();

      TimestampedEntry entry;
      while ((entry = getNextEntryForCleanup(cleanupTime)) != null) {
        destroy(entry.getElement());
      }
    } finally {
      isCleanupInProgress.set(false);
    }
  }

  // return the entry to cleanup, or null to signify that there is no element that needs cleaning up
  @Nullable
  private TimestampedEntry getNextEntryForCleanup(long cleanupTime) {
    // if the last element doesn't need expiry, simply return
    final TimestampedEntry last = elements.peekLast();
    if (last == null || cleanupTime - last.getTimestamp() < timeoutMillis) {
      return null;
    }

    final TimestampedEntry entry = elements.pollLast();
    if (entry == null) {
      return null;
    }
    if (cleanupTime - entry.getTimestamp() < timeoutMillis) {
      // add it back to the tail, if it doesn't require expiring
      // since cleanup happens only from one thread, and we add the single entry to the tail of the queue,
      // time ordering of the entries is maintained.
      elements.addLast(entry);
      return null;
    }
    return entry;
  }

  /**
   * Returns an element to the pool of available elements. The element must
   * have been obtained from this pool through obtain(). The recycle() method
   * is called before the element is available for obtain().
   *
   * @param element the element to be returned
   */
  public void release(T element) {
    recycle(element);
    elements.addFirst(new TimestampedEntry(element, System.currentTimeMillis()));
  }

  /**
   * Discard an element from the pool. The element must have been obtained
   * from this pool. The destroy() method will be called.
   *
   * @param element the element to be discarded
   */
  public void discard(T element) {
    destroy(element);
  }

  /**
   * If the pool has an available element, return it. Otherwise,
   * create a new element and return it.
   * @return An element of type {@link T}.
   */
  private T getOrCreate() throws E {
    TimestampedEntry entry = elements.pollFirst();
    if (entry != null) {
      // a entry was available, all good
      return entry.getElement();
    }
    return create();
  }
}
