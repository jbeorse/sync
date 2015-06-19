/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.sync.service.impl;

import org.opendatakit.sync.AbstractSyncServiceTest;
import org.opendatakit.sync.service.OdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncStatus;


public class SyncServiceTest extends AbstractSyncServiceTest {

	public SyncServiceTest() {
		super();
	}


	public void testBinding() {
		OdkSyncServiceInterface odkSyncServiceInterface = bindToService();

		assertStatusCorrect(odkSyncServiceInterface, SyncStatus.INIT);

	}

	public void testFailureForCheckins() {
	  assertTrue(true);
	}
	
	
//	public void testRunning() {
//		setupService();
//
//		try {
//			OdkSyncServiceInterface odkSyncServiceInterface = bindToService();
//			odkSyncServiceInterface.synchronize();
//			assertStatusCorrect(odkSyncServiceInterface, SyncStatus.SYNC_COMPLETE);
//		} catch (RemoteException e) {
//			assertTrue(false);
//		}
//		
//		shutdownService();
//	}
}
