package org.infinispan.client.hotrod.proto;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class ListOfString {
   private final List<String> list;

   public ListOfString(List<String> list) {
       this.list = list;
    }

   @ProtoFactory
   static <E> ListOfString protoFactory(List<String> list) {
      return new ListOfString(list);
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   public List<String> getList() {
      return list;
   }
}
