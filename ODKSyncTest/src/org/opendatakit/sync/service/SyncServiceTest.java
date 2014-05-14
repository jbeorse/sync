package org.opendatakit.sync.service;

import org.opendatakit.sync.AbstractSyncServiceTest;


public class SyncServiceTest extends AbstractSyncServiceTest {

	public SyncServiceTest() {
		super();
	}


	public void testBinding() {
		OdkSyncServiceInterface odkSyncServiceInterface = bindToService();

		assertStatusCorrect(odkSyncServiceInterface, SyncStatus.INIT);

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
