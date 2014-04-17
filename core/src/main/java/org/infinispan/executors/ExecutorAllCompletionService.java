package org.infinispan.executors;

import net.jcip.annotations.GuardedBy;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Executes given tasks in provided executor.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @author Dan Berindei
 */
public class ExecutorAllCompletionService<V> {
   private final Lock completedLock = new ReentrantLock();
   private final Condition completedCondition = completedLock.newCondition();
   private Executor executor;
   private AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
   private AtomicInteger scheduled = new AtomicInteger();
   @GuardedBy("completedLock")
   private volatile int completed = 0;

   public ExecutorAllCompletionService(Executor executor) {
      this.executor = executor;
   }

   public Future<V> submit(final Callable<V> task) {
      scheduled.incrementAndGet();
      CustomFutureTask<V> futureTask = new CustomFutureTask<V>(task);
      executor.execute(futureTask);
      return futureTask;
   }

   public Future<V> submit(final Runnable task, V result) {
      scheduled.incrementAndGet();
      CustomFutureTask<V> futureTask = new CustomFutureTask<V>(task, result);
      executor.execute(futureTask);
      return futureTask;
   }

   /**
    * @return True if all currently scheduled tasks have already been completed, false otherwise;
    */
   public boolean isAllCompleted() {
      return completed >= scheduled.get();
   }

   public void waitUntilAllCompleted() throws InterruptedException {
      if (isAllCompleted())
         return;

      completedLock.lock();
      try {
         while (completed < scheduled.get()) {
            completedCondition.await();
         }
      } finally {
         completedLock.unlock();
      }
   }

   public boolean isExceptionThrown() {
      return firstException.get() != null;
   }

   public ExecutionException getFirstException() {
      return new ExecutionException(firstException.get());
   }

   private class CustomFutureTask<V> extends java.util.concurrent.FutureTask<V> {
      public CustomFutureTask(Callable<V> task) {
         super(task);
      }

      public CustomFutureTask(Runnable task, V result) {
         super(task, result);
      }

      @Override
      protected void done() {
         completedLock.lock();
         try {
            completed++;
            if (completed >= scheduled.get()) {
               completedCondition.signalAll();
            }
         } finally {
            completedLock.unlock();
         }
      }

      @Override
      protected void setException(Throwable t) {
         firstException.compareAndSet(null, t);
         super.setException(t);
      }
   }
}
