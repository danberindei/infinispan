package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.ReplicableCommand;

public class ClusterJoinCommand implements ReplicableCommand {
   private static final byte COMMAND_ID = 81;

   private short version;

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeShort(version);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      version = input.readShort();
   }
}
