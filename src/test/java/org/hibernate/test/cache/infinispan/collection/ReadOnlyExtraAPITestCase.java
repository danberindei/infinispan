/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or it's affiliates, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.hibernate.test.cache.infinispan.collection;
import org.hibernate.cache.access.AccessType;

import org.junit.Test;

import static org.junit.Assert.fail;
/**
 * ReadOnlyExtraAPITestCase.
 *
 * @author Galder Zamarreño
 * @since 3.5
 */
public class ReadOnlyExtraAPITestCase extends TransactionalExtraAPITestCase {
	@Override
	protected AccessType getAccessType() {
		return AccessType.READ_ONLY;
	}

	@Test
	@Override
	public void testLockItem() {
		try {
			getCollectionAccessStrategy().lockItem( KEY, new Integer( 1 ) );
			fail( "Call to lockItem did not throw exception" );
		}
		catch (UnsupportedOperationException expected) {
		}
	}

	@Test
	@Override
	public void testLockRegion() {
		try {
			getCollectionAccessStrategy().lockRegion();
			fail( "Call to lockRegion did not throw exception" );
		}
		catch (UnsupportedOperationException expected) {
		}
	}

}
