/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.server.hotrod.test;

import java.io.IOException;
import java.util.Random;

import org.infinispan.Cache;
import org.infinispan.affinity.KeyGenerator;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.JBossMarshaller;
import org.infinispan.remoting.transport.Address;

/**
 * {@link org.infinispan.distribution.MagicKey} equivalent for HotRod
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class HotRodMagicKeyGenerator {
   public static byte[] newKey(Cache cache) {
      ConsistentHash ch = cache.getAdvancedCache().getDistributionManager().getReadConsistentHash();
      Address nodeAddress = cache.getAdvancedCache().getRpcManager().getAddress();
      Random r = new Random();

      for (int i = 0; i < 1000; i++) {
         String candidate = String.valueOf(r.nextLong());
         Marshaller sm = new JBossMarshaller();
         try {
            byte[] candidateBytes = sm.objectToByteBuffer(candidate, 64);
            if (ch.isKeyLocalToNode(nodeAddress, candidateBytes)) {
               return candidateBytes;
            }
         } catch (IOException e) {
            throw new RuntimeException(e);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }

      throw new RuntimeException("Unable to find a key local to node " + cache);
   }

   public static String getStringObject(byte[] bytes) {
      try {
         Marshaller sm = new JBossMarshaller();
         return (String) sm.objectFromByteBuffer(bytes);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
