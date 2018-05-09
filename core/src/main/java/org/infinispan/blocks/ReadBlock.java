package org.infinispan.blocks;

import org.infinispan.interceptors.InvocationStage;

public interface ReadBlock {
   InvocationStage fetchEntry(Object key, long flagsBitSet);
   InvocationStage fetchEntries(Object key);
}
