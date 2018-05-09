package org.infinispan.blocks;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.ModificationsList;

public interface ClusteringBlock {
   InvocationStage fetchRemote(Object key, long flagsBitSet);
   InvocationStage updateRemote(Collection<ModificationsList> entries, long flagsBitSet);
}
