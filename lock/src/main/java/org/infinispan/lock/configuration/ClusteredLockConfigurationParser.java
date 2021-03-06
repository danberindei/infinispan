package org.infinispan.lock.configuration;

import static org.infinispan.lock.configuration.ClusteredLockConfigurationParser.NAMESPACE;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.lock.logging.Log;
import org.kohsuke.MetaInfServices;

/**
 * Clustered Locks configuration parser
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
@MetaInfServices
@Namespace(root = "clustered-locks")
@Namespace(uri = NAMESPACE + "*", root = "clustered-locks", since = "9.4")
public class ClusteredLockConfigurationParser implements ConfigurationParser {

   static final String NAMESPACE = Parser.NAMESPACE + "clustered-locks:";

   private static final Log log = LogFactory.getLog(ClusteredLockConfigurationParser.class, Log.class);

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (!holder.inScope(ParserScope.CACHE_CONTAINER)) {
         throw log.invalidScope(holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case CLUSTERED_LOCKS: {
            parseClusteredLocksElement(reader, builder.addModule(ClusteredLockManagerConfigurationBuilder.class));
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   private void parseClusteredLocksElement(ConfigurationReader reader, ClusteredLockManagerConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NUM_OWNERS:
               builder.numOwner(Integer.parseInt(value));
               break;
            case RELIABILITY:
               builder.reliability(Reliability.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLUSTERED_LOCK:
               parseClusteredLock(reader, builder.addClusteredLock());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseClusteredLock(ConfigurationReader reader, ClusteredLockConfigurationBuilder builder)  {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME:
               builder.name(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }
}
