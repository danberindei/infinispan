/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.test;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;


public class ResponseReceivedWithoutRetvalTest {
   private static Log log = LogFactory.getLog(ResponseReceivedWithoutRetvalTest.class);
   private static Random random = new Random(System.currentTimeMillis());
   private static final int NODES = 10;

   public static void main(String[] args) throws Exception {
      CountDownLatch latch = new CountDownLatch(1);
      JChannel[] chs = new JChannel[NODES];
      RpcDispatcher[] dispatchers = new RpcDispatcher[NODES];

      for (int i = 0; i < NODES; i++) {
         chs[i] = createChannel("demo" + i);
         dispatchers[i] = new RpcDispatcher(chs[i], new RpcServer(chs[i], latch));
         chs[i].connect("demo");
      }

      while (chs[0].getView().size() < NODES)
         Thread.sleep(100);

      org.jgroups.Address ch0Address = chs[0].getAddress();
      log.info("Stopping " + chs[0].getAddress());
      chs[0].disconnect();

      final MethodCall call = new MethodCall(RpcServer.class.getMethod("doSomething"));
      for (int i = 1; i < NODES; i++) {
         final JChannel ch = chs[i];
         final RpcDispatcher disp = dispatchers[i];
         final Collection<org.jgroups.Address> targets = Collections.singleton(ch0Address);
         new Thread(){
            @Override
            public void run() {
               try {
                  log.info("Sending RPC from " + ch.getAddress());
                  RspList<Object> result = disp.callRemoteMethods(targets, call, RequestOptions.SYNC());
                  //RspList<Object> result = disp.callRemoteMethods(null, call, RequestOptions.SYNC());
                  log.info("Received RPC responses on " + ch.getAddress() + ": " + result);
                  for (Rsp<Object> rsp : result) {
                     if (rsp.wasReceived() &&!rsp.wasSuspected() && rsp.getValue() == null) {
                        log.error("Received response without retval");
                     }
                  }
               } catch (Throwable t) {
                  log.error("Error in RPC", t);
               }
            }
         }.start();
      }

      //Thread.sleep(50);
      latch.countDown();

      log.info("Waiting to receive responses for all RPCs");
      Thread.sleep(15000);
      log.info("Shutting down");
      System.exit(0);
   }

   private static JChannel createChannel(String name) throws Exception {
      final JChannel ch = new JChannel();
      ch.setName(name);
      return ch;
   }

   public static class RpcServer {
      private final JChannel ch;
      private final CountDownLatch latch;

      public RpcServer(JChannel ch, CountDownLatch latch) {
         this.ch = ch;
         this.latch = latch;
      }

      public String doSomething() {
         //log.info("Got call on " + ch.getAddress());
         try {
            //Thread.sleep(random.nextInt(100));
            latch.await();
         } catch (InterruptedException e) {
            return "interrupted";
         }
         //return "Something-" + ch.getAddress();
         throw new RuntimeException("Something-" + ch.getAddress());
      }
   }
}
