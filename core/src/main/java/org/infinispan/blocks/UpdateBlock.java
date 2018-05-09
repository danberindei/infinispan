package org.infinispan.blocks;

import java.util.Collection;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.interceptors.InvocationStage;

public interface UpdateBlock {
   InvocationStage updateEntry(InternalCacheEntry entry);
   InvocationStage updateEntries(Collection<InternalCacheEntry> entries);
}
