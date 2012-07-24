/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.transaction;

import org.infinispan.CacheException;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Object that holds transaction's state on the node where it originated; as opposed to {@link RemoteTransaction}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.0
 */
public abstract class LocalTransaction extends AbstractCacheTransaction {

   private static final Log log = LogFactory.getLog(LocalTransaction.class);
   private static final boolean trace = log.isTraceEnabled();

   private Set<Address> remoteLockedNodes;
   protected Set<Object> readKeys = null;

   /** mark as volatile as this might be set from the tx thread code on view change*/
   private volatile boolean isMarkedForRollback;

   private final Transaction transaction;

   private final boolean implicitTransaction;

   //total order result -- has the result and behaves like a synchronization point between the remote and local
   // prepare commands
   private final PrepareResult prepareResult = new PrepareResult();

   public LocalTransaction(Transaction transaction, GlobalTransaction tx, boolean implicitTransaction, int viewId) {
      super(tx, viewId);
      this.transaction = transaction;
      this.implicitTransaction = implicitTransaction;
   }

   public void addModification(WriteCommand mod) {
      if (trace) log.tracef("Adding modification %s. Mod list is %s", mod, modifications);
      if (modifications == null) {
         modifications = new LinkedList<WriteCommand>();
      }
      modifications.add(mod);
   }

   public void locksAcquired(Collection<Address> nodes) {
      log.tracef("Adding remote locks on %s. Remote locks are %s", nodes, remoteLockedNodes);
      if (remoteLockedNodes == null)
         remoteLockedNodes = new HashSet<Address>(nodes);
      else
         remoteLockedNodes.addAll(nodes);
   }

   public Collection<Address> getRemoteLocksAcquired() {
      if (remoteLockedNodes == null) return Collections.emptySet();
      return remoteLockedNodes;
   }

   public void clearRemoteLocksAcquired() {
      if (remoteLockedNodes != null) remoteLockedNodes.clear();
   }

   public void markForRollback(boolean markForRollback) {
      isMarkedForRollback = markForRollback;
   }

   public final boolean isMarkedForRollback() {
      return isMarkedForRollback;
   }

   public Transaction getTransaction() {
      return transaction;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return (Map<Object, CacheEntry>)
            (lookedUpEntries == null ? Collections.emptyMap() : lookedUpEntries);
   }

   public boolean isImplicitTransaction() {
      return implicitTransaction;
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      if (isMarkedForRollback()) {
         throw new CacheException("This transaction is marked for rollback and cannot acquire locks!");
      }
      if (lookedUpEntries == null) lookedUpEntries = new HashMap<Object, CacheEntry>(4);
      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      if (isMarkedForRollback()) {
         throw new CacheException("This transaction is marked for rollback and cannot acquire locks!");
      }
      if (lookedUpEntries == null) {
         lookedUpEntries = new HashMap<Object, CacheEntry>(entries);
      } else {
         lookedUpEntries.putAll(entries);
      }
   }

   public boolean isReadOnly() {
      return (modifications == null || modifications.isEmpty()) && (lookedUpEntries == null || lookedUpEntries.isEmpty());
   }

   public abstract boolean isEnlisted();

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LocalTransaction that = (LocalTransaction) o;

      return tx.getId() == that.tx.getId();
   }

   @Override
   public int hashCode() {
      long id = tx.getId();
      return (int) (id ^ (id >>> 32));
   }

   @Override
   public String toString() {
      return "LocalTransaction{" +
            "remoteLockedNodes=" + remoteLockedNodes +
            ", isMarkedForRollback=" + isMarkedForRollback +
            ", transaction=" + transaction +
            ", lockedKeys=" + lockedKeys +
            ", backupKeyLocks=" + backupKeyLocks +
            ", viewId=" + viewId +
            "} " + super.toString();
   }

   public void setModifications(List<WriteCommand> modifications) {
      this.modifications = modifications;
   }

   @Override
   public void addReadKey(Object key) {
      if (readKeys == null) readKeys = new HashSet<Object>(2);
      readKeys.add(key);
   }

   @Override
   public boolean keyRead(Object key) {
      return readKeys != null && readKeys.contains(key);
   }

   /**
    * Total order result
    */
   private class PrepareResult {
      //modifications are applied?
      private volatile boolean modificationsApplied;
      //is the result an exception?
      private volatile boolean exception;
      //the validation result
      private volatile Object result;

      //for distribution:
      //has a set of keys that are not validated yet
      private Set<Object> keysMissingValidation;
      //true if the transaction was locally prepared
      private volatile boolean localPrepared;

      //for state transfer
      private volatile boolean markedToRetransmit;

      public PrepareResult() {}

      private synchronized void init(Collection<Object> affectedKeys) {
         keysMissingValidation = new HashSet<Object>(affectedKeys);
      }

      private synchronized void waitForOutcome() throws Throwable {
         if (!modificationsApplied && !markedToRetransmit) {
            this.wait();
         }
         if (markedToRetransmit) {
            //no op
         } else if (!modificationsApplied) {
            throw new TimeoutException("Unable to wait until modifications are applied");
         } else if (exception) {
            throw (Throwable) result;
         }
      }

      private synchronized void addResultFromPrepare(Object result, boolean exception) {
         log.tracef("Received prepare result %s, is exception? %s", result, exception);

         modificationsApplied = true;
         this.result = result;
         this.exception = exception;

         if (!exception && result != null && result instanceof EntryVersionsMap) {
            setUpdatedEntryVersions(((EntryVersionsMap) result).merge(getUpdatedEntryVersions()));
         }

         this.notify();
      }

      private synchronized void addKeysValidated(Collection<Object> keysValidated, boolean local) {
         if (modificationsApplied) {
            return; //already prepared
         }

         keysMissingValidation.removeAll(keysValidated);
         if (local) {
            prepareResult.localPrepared = true;
         }
         checkIfPrepared();
      }

      private synchronized void addException(Exception exception, boolean local) {
         if (modificationsApplied) {
            return; //already prepared
         }

         result = exception;
         this.exception = true;
         keysMissingValidation = Collections.emptySet();

         if (local) {
            localPrepared = true;
         }
         checkIfPrepared();
      }

      private synchronized void markToRetransmit() {
         markedToRetransmit = true;
         this.notify();
      }

      private synchronized boolean isMarkedToRetransmitAndReset() {
         boolean marked = markedToRetransmit;
         markedToRetransmit = false;
         return marked;
      }

      /**
       * unblocks the thread if enough conditions are ok to mark the transaction as prepared
       */
      private void checkIfPrepared() {
         //the condition is: the transaction was delivered locally and all keys are validated (or an exception)
         if (prepareResult.localPrepared && prepareResult.keysMissingValidation.isEmpty()) {
            prepareResult.modificationsApplied = true;
            this.notify();
         }
      }
   }

   /**
    * waits until the modification are applied
    *
    * @throws Throwable throw the validation result if it is an exception
    */
   public final void awaitUntilModificationsApplied() throws Throwable {
      prepareResult.waitForOutcome();
   }

   /**
    * add the transaction result and notify
    *
    * @param object    the validation result
    * @param exception is it an exception?
    */
   public void addPrepareResult(Object object, boolean exception) {
      prepareResult.addResultFromPrepare(object, exception);      
   }

   /**
    * add a collection of keys successful validated (write skew check)
    * @param keysValidated the keys validated
    * @param local         if it is originated locally or remotely
    */
   public final void addKeysValidated(Collection<Object> keysValidated, boolean local) {
      prepareResult.addKeysValidated(keysValidated, local);
   }

   /**
    * add an exception (write skew check fails)
    * @param exception  the exception
    * @param local      if it is originated locally or remotely
    */
   public final void addException(Exception exception, boolean local) {
      prepareResult.addException(exception, local);
   }

   /**
    * invoked before send the prepare command to init the collection of keys that needs validation
    * @param affectedKeys  the collection of keys wrote by the transaction
    */
   public final void initToCollectAcks(Collection<Object> affectedKeys) {
      prepareResult.init(affectedKeys);
   }

   public final void markToRetransmit() {
      prepareResult.markToRetransmit();
   }

   public final boolean isMarkedToRetransmit() {
      return prepareResult.isMarkedToRetransmitAndReset();
   }
}
