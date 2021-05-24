package org.infinispan.client.hotrod.proto;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(includeClasses = {GrowthStats.class, ListOfWrappedLists.class}, schemaPackageName = "org.infinispan.client.hotrod.proto")
public interface GrowthContextInitializer extends SerializationContextInitializer {
}
