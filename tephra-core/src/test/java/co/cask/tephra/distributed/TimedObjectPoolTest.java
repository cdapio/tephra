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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests {@link TimedObjectPool}.
 */
public class TimedObjectPoolTest {

  private static final class ElementWithId {
    private final int id;

    public ElementWithId(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }
  }

  private static final class ElementWithIdPool extends TimedObjectPool<ElementWithId, Exception> {

    private int counter;

    public ElementWithIdPool(long timeoutMillis) {
      super(timeoutMillis);
      this.counter = 0;
    }

    @Override
    protected ElementWithId create() throws Exception {
      return new ElementWithId(counter++);
    }
  }

  @Test
  public void test() throws Exception {
    final int timeoutInMillis = 50;
    ElementWithIdPool pool = new ElementWithIdPool(timeoutInMillis);
    ElementWithId obtain0 = pool.obtain();
    Assert.assertEquals(0, obtain0.getId());
    ElementWithId obtain1 = pool.obtain();
    Assert.assertEquals(1, obtain1.getId());
    ElementWithId obtain2 = pool.obtain();
    Assert.assertEquals(2, obtain2.getId());

    // release them in order: 0, 1, 2 and so the following calls to obtain() should return them in order: 2, 1, 0 (FILO)
    pool.release(obtain0);
    pool.release(obtain1);
    pool.release(obtain2);

    obtain0 = pool.obtain();
    obtain1 = pool.obtain();
    obtain2 = pool.obtain();

    Assert.assertEquals(2, obtain0.getId());
    Assert.assertEquals(1, obtain1.getId());
    Assert.assertEquals(0, obtain2.getId());

    // release id='2' and wait twice the timeout, to ensure it qualifies for cleanup.
    pool.release(obtain0);
    TimeUnit.MILLISECONDS.sleep(timeoutInMillis * 2);

    pool.release(obtain1);
    pool.release(obtain2);

    // id '2' was destroyed due to the timeout, so we should get: 0, 1, 3
    obtain0 = pool.obtain();
    obtain1 = pool.obtain();
    obtain2 = pool.obtain();

    Assert.assertEquals(0, obtain0.getId());
    Assert.assertEquals(1, obtain1.getId());
    Assert.assertEquals(3, obtain2.getId());
  }
}
