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
