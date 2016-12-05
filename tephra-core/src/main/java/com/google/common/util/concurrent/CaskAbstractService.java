/*
 * Copyright © 2016 Cask Data, Inc.
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

package com.google.common.util.concurrent;

import com.google.common.annotations.Beta;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * <p>
 * {@link Service} interface in guava 12.0.1 does not support adding listeners to the
 * service state changes. {@link CaskService} class is copy of the {@code Service} interface
 * from guava 13.0.1 which has support for the service listeners. {@code AbstractCaskService}
 * is a copy of the {@link AbstractService} from guava 13.0.1 which implements the
 * {@code CaskService}.
 * </p>
 *
 * Base class for implementing services that can handle {@link #doStart} and {@link #doStop}
 * requests, responding to them with {@link #notifyStarted()} and {@link #notifyStopped()}
 * callbacks. Its subclasses must manage threads manually; consider
 * {@link AbstractExecutionThreadService} if you need only a single execution thread.
 *
 * @author Jesse Wilson
 * @author Luke Sandberg
 * @since 1.0
 */
@Beta
public abstract class CaskAbstractService implements CaskService {
  private static final Logger logger = Logger.getLogger(CaskAbstractService.class.getName());
  private final ReentrantLock lock = new ReentrantLock();

  private final Transition startup = new Transition();
  private final Transition shutdown = new Transition();

  /**
   * The listeners to notify during a state transition.
   */
  @GuardedBy("lock")
  private final List<ListenerExecutorPair> listeners = Lists.newArrayList();

  /**
   * The queue of listeners that are waiting to be executed.
   *
   * <p>Enqueue operations should be protected by {@link #lock} while dequeue operations should be
   * protected by the implicit lock on this object. Dequeue operations should be executed atomically
   * with the execution of the {@link Runnable} and additionally the {@link #lock} should not be
   * held when the listeners are being executed. Use {@link #executeListeners} for this operation.
   * This is necessary to ensure that elements on the queue are executed in the correct order.
   * Enqueue operations should be protected so that listeners are added in the correct order. We use
   * a concurrent queue implementation so that enqueues can be executed concurrently with dequeues.
   */
  @GuardedBy("queuedListeners")
  private final Queue<Runnable> queuedListeners = Queues.newConcurrentLinkedQueue();

  /**
   * The current state of the service.  This should be written with the lock held but can be read
   * without it because it is an immutable object in a volatile field.  This is desirable so that
   * methods like {@link #state}, and notably {@link #toString} can be run
   * without grabbing the lock.
   *
   * <p>To update this field correctly the lock must be held to guarantee that the state is
   * consistent.
   */
  @GuardedBy("lock")
  private volatile StateSnapshot snapshot = new StateSnapshot(Service.State.NEW);

  protected CaskAbstractService() {
    // Add a listener to update the futures. This needs to be added first so that it is executed
    // before the other listeners. This way the other listeners can access the completed futures.
    addListener(
      new Listener() {
        @Override public void starting() {}

        @Override public void running() {
          startup.set(Service.State.RUNNING);
        }

        @Override public void stopping(Service.State from) {
          if (from == Service.State.STARTING) {
            startup.set(Service.State.STOPPING);
          }
        }

        @Override public void terminated(Service.State from) {
          if (from == Service.State.NEW) {
            startup.set(Service.State.TERMINATED);
          }
          shutdown.set(Service.State.TERMINATED);
        }

        @Override public void failed(Service.State from, Throwable failure) {
          switch (from) {
            case STARTING:
              startup.setException(failure);
              shutdown.setException(new Exception("Service failed to start.", failure));
              break;
            case RUNNING:
              shutdown.setException(new Exception("Service failed while running", failure));
              break;
            case STOPPING:
              shutdown.setException(failure);
              break;
            case TERMINATED:  /* fall-through */
            case FAILED:  /* fall-through */
            case NEW:  /* fall-through */
            default:
              throw new AssertionError("Unexpected from state: " + from);
          }
        }
      },
      MoreExecutors.sameThreadExecutor());
  }

  /**
   * This method is called by {@link #start} to initiate service startup. The invocation of this
   * method should cause a call to {@link #notifyStarted()}, either during this method's run, or
   * after it has returned. If startup fails, the invocation should cause a call to
   * {@link #notifyFailed(Throwable)} instead.
   *
   * <p>This method should return promptly; prefer to do work on a different thread where it is
   * convenient. It is invoked exactly once on service startup, even when {@link #start} is called
   * multiple times.
   */
  protected abstract void doStart();

  /**
   * This method should be used to initiate service shutdown. The invocation of this method should
   * cause a call to {@link #notifyStopped()}, either during this method's run, or after it has
   * returned. If shutdown fails, the invocation should cause a call to
   * {@link #notifyFailed(Throwable)} instead.
   *
   * <p> This method should return promptly; prefer to do work on a different thread where it is
   * convenient. It is invoked exactly once on service shutdown, even when {@link #stop} is called
   * multiple times.
   */
  protected abstract void doStop();

  @Override
  public final ListenableFuture<Service.State> start() {
    lock.lock();
    try {
      if (snapshot.state == Service.State.NEW) {
        snapshot = new StateSnapshot(Service.State.STARTING);
        starting();
        doStart();
      }
    } catch (Throwable startupFailure) {
      notifyFailed(startupFailure);
    } finally {
      lock.unlock();
      executeListeners();
    }

    return startup;
  }

  @Override
  public final ListenableFuture<Service.State> stop() {
    lock.lock();
    try {
      switch (snapshot.state) {
        case NEW:
          snapshot = new StateSnapshot(Service.State.TERMINATED);
          terminated(Service.State.NEW);
          break;
        case STARTING:
          snapshot = new StateSnapshot(Service.State.STARTING, true, null);
          stopping(Service.State.STARTING);
          break;
        case RUNNING:
          snapshot = new StateSnapshot(Service.State.STOPPING);
          stopping(Service.State.RUNNING);
          doStop();
          break;
        case STOPPING:
        case TERMINATED:
        case FAILED:
          // do nothing
          break;
        default:
          throw new AssertionError("Unexpected state: " + snapshot.state);
      }
    } catch (Throwable shutdownFailure) {
      notifyFailed(shutdownFailure);
    } finally {
      lock.unlock();
      executeListeners();
    }

    return shutdown;
  }

  @Override
  public Service.State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public Service.State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  /**
   * Implementing classes should invoke this method once their service has started. It will cause
   * the service to transition from {@link Service.State#STARTING} to {@link Service.State#RUNNING}.
   *
   * @throws IllegalStateException if the service is not {@link Service.State#STARTING}.
   */
  protected final void notifyStarted() {
    lock.lock();
    try {
      if (snapshot.state != Service.State.STARTING) {
        IllegalStateException failure = new IllegalStateException(
          "Cannot notifyStarted() when the service is " + snapshot.state);
        notifyFailed(failure);
        throw failure;
      }

      if (snapshot.shutdownWhenStartupFinishes) {
        snapshot = new StateSnapshot(Service.State.STOPPING);
        // We don't call listeners here because we already did that when we set the
        // shutdownWhenStartupFinishes flag.
        doStop();
      } else {
        snapshot = new StateSnapshot(Service.State.RUNNING);
        running();
      }
    } finally {
      lock.unlock();
      executeListeners();
    }
  }

  /**
   * Implementing classes should invoke this method once their service has stopped. It will cause
   * the service to transition from {@link Service.State#STOPPING} to {@link Service.State#TERMINATED}.
   *
   * @throws IllegalStateException if the service is neither {@link Service.State#STOPPING} nor
   *         {@link Service.State#RUNNING}.
   */
  protected final void notifyStopped() {
    lock.lock();
    try {
      if (snapshot.state != Service.State.STOPPING && snapshot.state != Service.State.RUNNING) {
        IllegalStateException failure = new IllegalStateException(
          "Cannot notifyStopped() when the service is " + snapshot.state);
        notifyFailed(failure);
        throw failure;
      }
      Service.State previous = snapshot.state;
      snapshot = new StateSnapshot(Service.State.TERMINATED);
      terminated(previous);
    } finally {
      lock.unlock();
      executeListeners();
    }
  }

  /**
   * Invoke this method to transition the service to the {@link Service.State#FAILED}. The service will
   * <b>not be stopped</b> if it is running. Invoke this method when a service has failed critically
   * or otherwise cannot be started nor stopped.
   */
  protected final void notifyFailed(Throwable cause) {
    checkNotNull(cause);

    lock.lock();
    try {
      switch (snapshot.state) {
        case NEW:
        case TERMINATED:
          throw new IllegalStateException("Failed while in state:" + snapshot.state, cause);
        case RUNNING:
        case STARTING:
        case STOPPING:
          Service.State previous = snapshot.state;
          snapshot = new StateSnapshot(Service.State.FAILED, false, cause);
          failed(previous, cause);
          break;
        case FAILED:
          // Do nothing
          break;
        default:
          throw new AssertionError("Unexpected state: " + snapshot.state);
      }
    } finally {
      lock.unlock();
      executeListeners();
    }
  }

  @Override
  public final boolean isRunning() {
    return state() == Service.State.RUNNING;
  }

  @Override
  public final Service.State state() {
    return snapshot.externalState();
  }

  @Override
  public final void addListener(Listener listener, Executor executor) {
    checkNotNull(listener, "listener");
    checkNotNull(executor, "executor");
    lock.lock();
    try {
      if (snapshot.state != Service.State.TERMINATED && snapshot.state != Service.State.FAILED) {
        listeners.add(new ListenerExecutorPair(listener, executor));
      }
    } finally {
      lock.unlock();
    }
  }

  @Override public String toString() {
    return getClass().getSimpleName() + " [" + state() + "]";
  }

  /**
   * A change from one service state to another, plus the result of the change.
   */
  private class Transition extends AbstractFuture<Service.State> {
    @Override
    public Service.State get(long timeout, TimeUnit unit)
      throws InterruptedException, TimeoutException, ExecutionException {
      try {
        return super.get(timeout, unit);
      } catch (TimeoutException e) {
        throw new TimeoutException(CaskAbstractService.this.toString());
      }
    }
  }

  /**
   * Attempts to execute all the listeners in {@link #queuedListeners} while not holding the
   * {@link #lock}.
   */
  private void executeListeners() {
    if (!lock.isHeldByCurrentThread()) {
      synchronized (queuedListeners) {
        Runnable listener;
        while ((listener = queuedListeners.poll()) != null) {
          listener.run();
        }
      }
    }
  }

  @GuardedBy("lock")
  private void starting() {
    for (final ListenerExecutorPair pair : listeners) {
      queuedListeners.add(new Runnable() {
        @Override public void run() {
          pair.execute(new Runnable() {
            @Override public void run() {
              pair.listener.starting();
            }
          });
        }
      });
    }
  }

  @GuardedBy("lock")
  private void running() {
    for (final ListenerExecutorPair pair : listeners) {
      queuedListeners.add(new Runnable() {
        @Override public void run() {
          pair.execute(new Runnable() {
            @Override public void run() {
              pair.listener.running();
            }
          });
        }
      });
    }
  }

  @GuardedBy("lock")
  private void stopping(final Service.State from) {
    for (final ListenerExecutorPair pair : listeners) {
      queuedListeners.add(new Runnable() {
        @Override public void run() {
          pair.execute(new Runnable() {
            @Override public void run() {
              pair.listener.stopping(from);
            }
          });
        }
      });
    }
  }

  @GuardedBy("lock")
  private void terminated(final Service.State from) {
    for (final ListenerExecutorPair pair : listeners) {
      queuedListeners.add(new Runnable() {
        @Override public void run() {
          pair.execute(new Runnable() {
            @Override public void run() {
              pair.listener.terminated(from);
            }
          });
        }
      });
    }
    // There are no more state transitions so we can clear this out.
    listeners.clear();
  }

  @GuardedBy("lock")
  private void failed(final Service.State from, final Throwable cause) {
    for (final ListenerExecutorPair pair : listeners) {
      queuedListeners.add(new Runnable() {
        @Override public void run() {
          pair.execute(new Runnable() {
            @Override public void run() {
              pair.listener.failed(from, cause);
            }
          });
        }
      });
    }
    // There are no more state transitions so we can clear this out.
    listeners.clear();
  }

  /** A simple holder for a listener and its executor. */
  private static class ListenerExecutorPair {
    final Listener listener;
    final Executor executor;

    ListenerExecutorPair(Listener listener, Executor executor) {
      this.listener = listener;
      this.executor = executor;
    }

    /**
     * Executes the given {@link Runnable} on {@link #executor} logging and swallowing all
     * exceptions
     */
    void execute(Runnable runnable) {
      try {
        executor.execute(runnable);
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Exception while executing listener " + listener
          + " with executor " + executor, e);
      }
    }
  }

  /**
   * An immutable snapshot of the current state of the service. This class represents a consistent
   * snapshot of the state and therefore it can be used to answer simple queries without needing to
   * grab a lock.
   */
  @Immutable
  private static final class StateSnapshot {
    /**
     * The internal state, which equals external state unless
     * shutdownWhenStartupFinishes is true.
     */
    final Service.State state;

    /**
     * If true, the user requested a shutdown while the service was still starting
     * up.
     */
    final boolean shutdownWhenStartupFinishes;

    /**
     * The exception that caused this service to fail.  This will be {@code null}
     * unless the service has failed.
     */
    @Nullable
    final Throwable failure;

    StateSnapshot(Service.State internalState) {
      this(internalState, false, null);
    }

    StateSnapshot(Service.State internalState, boolean shutdownWhenStartupFinishes, Throwable failure) {
      checkArgument(!shutdownWhenStartupFinishes || internalState == Service.State.STARTING,
                    "shudownWhenStartupFinishes can only be set if Service.State is STARTING. Got %s instead.",
                    internalState);
      checkArgument(!(failure != null ^ internalState == Service.State.FAILED),
                    "A failure cause should be set if and only if the state is failed.  Got %s and %s "
                      + "instead.", internalState, failure);
      this.state = internalState;
      this.shutdownWhenStartupFinishes = shutdownWhenStartupFinishes;
      this.failure = failure;
    }

    /** @see CaskService#state() */
    Service.State externalState() {
      if (shutdownWhenStartupFinishes && state == Service.State.STARTING) {
        return Service.State.STOPPING;
      } else {
        return state;
      }
    }

    Throwable failureCause() {
      checkState(state == Service.State.FAILED,
                 "failureCause() is only valid if the service has failed, service is %s", state);
      return failure;
    }
  }
}
