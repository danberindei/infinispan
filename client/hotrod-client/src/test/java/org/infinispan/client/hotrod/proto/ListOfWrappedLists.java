package org.infinispan.client.hotrod.proto;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class ListOfWrappedLists<E> {
    final List<List<E>> lists;

    public ListOfWrappedLists(List<List<E>> lists) {
       this.lists = lists;
    }

   public List<List<E>> getLists() {
      return lists;
   }

    @ProtoFactory
    static <E> ListOfWrappedLists<E> protoFactory(List<WrappedMessage> wrappedLists) {
       List<List<E>> lists =
             wrappedLists.stream().map(e -> (List<E>) e.getValue()).collect(Collectors.toList());
       return new ListOfWrappedLists<>(lists);
    }

    @ProtoField(number = 1, collectionImplementation = ArrayList.class)
    List<WrappedMessage> getWrappedLists() {
        return lists.stream().map(e -> new WrappedMessage(e)).collect(Collectors.toList());
    }
}
