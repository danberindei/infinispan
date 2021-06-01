package org.infinispan.client.hotrod.proto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class YearData {
   private final List<String> list;

   public YearData(List<String> list) {
       this.list = list;
   }

   public List<String> getList() {
      return list;
   }

   @ProtoFactory
   static <E> YearData protoFactory(String a, String b, String c) {
      return new YearData(new ArrayList<>(Arrays.asList(a, b, c)));
   }

   @ProtoField(1)
   public String a() {
      return list.get(0);
   }

   @ProtoField(2)
   public String b() {
      return list.get(1);
   }

   @ProtoField(3)
   public String c() {
      return list.get(2);
   }
}
