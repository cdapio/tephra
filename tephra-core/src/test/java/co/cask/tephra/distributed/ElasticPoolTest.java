/*
 * Copyright © 2014 Cask Data, Inc.
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

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link ElasticPool}.
 */
public class ElasticPoolTest {

  static class Dummy {
    static AtomicInteger count = new AtomicInteger(0);
    boolean valid = true;
    Dummy() {
      count.incrementAndGet();
    }
    void markInvalid() {
      valid = false;
    }

    public boolean isValid() {
      return valid;
    }
  }

  class DummyPool extends ElasticPool<Dummy, RuntimeException> {

    public DummyPool(int sizeLimit) {
      super(sizeLimit);
    }

    @Override
    protected Dummy create() {
      return new Dummy();
    }

    @Override
    protected boolean recycle(Dummy element) {
      return element.isValid();
    }
  }

  @Test(timeout = 5000)
  public void testFewerThreadsThanElements() throws InterruptedException {
    final DummyPool pool = new DummyPool(5);
    Dummy.count.set(0);
    createAndRunThreads(2, pool, false);
    // we only ran 2 threads, so only 2 elements got created, even though pool size is 5
    Assert.assertEquals(2, Dummy.count.get());
  }

  @Test(timeout = 5000)
  public void testMoreThreadsThanElements() throws InterruptedException {
    final DummyPool pool = new DummyPool(2);
    Dummy.count.set(0);
    createAndRunThreads(5, pool, false);
    // even though we ran 5 threads, only 2 elements got created because pool size is 2
    Assert.assertEquals(2, Dummy.count.get());
  }

  @Test(timeout = 5000)
  public void testMoreThreadsThanElementsWithDiscard() throws InterruptedException {
    final DummyPool pool = new DummyPool(2);
    Dummy.count.set(0);
    int numThreads = 3;
    // pass 'true' as the last parameter, which results in the elements being discarded after each obtain() call.
    createAndRunThreads(numThreads, pool, true);
    // this results in (5 * numThreads) number of elements being created since each thread obtains a client 5 times.
    Assert.assertEquals(5 * numThreads, Dummy.count.get());
  }

  // Creates a list of threads which obtain a client from the pool, sleeps for a certain amount of time, and then
  // releases the client back to the pool, optionally marking it invalid before doing so. It repeats this five times.
  // Then, runs these threads to completion.
  private void createAndRunThreads(int numThreads, final DummyPool pool,
                                   final boolean discardAtEnd) throws InterruptedException {
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          for (int j = 0; j < 5; ++j) {
            Dummy dummy;
            try {
              dummy = pool.obtain();
            } catch (InterruptedException e) {
              throw Throwables.propagate(e);
            }
            try {
              Thread.sleep(10L);
            } catch (InterruptedException e) {
              // ignored
            }
            if (discardAtEnd) {
              dummy.markInvalid();
            }
            pool.release(dummy);
          }
        }
      };
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
  }
}
