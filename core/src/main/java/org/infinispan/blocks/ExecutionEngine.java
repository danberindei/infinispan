package org.infinispan.blocks;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.context.InvocationContext;

public interface ExecutionEngine {
   Object executeGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command);
}
