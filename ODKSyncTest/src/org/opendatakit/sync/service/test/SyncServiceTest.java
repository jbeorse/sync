package org.opendatakit.sync.service.test;

import org.opendatakit.sync.SyncConsts;
import org.opendatakit.sync.files.SyncUtil;
import org.opendatakit.sync.service.OdkSyncService;
import org.opendatakit.sync.service.OdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncStatus;

import android.content.Intent;
import android.os.IBinder;
import android.os.RemoteException;
import android.test.ServiceTestCase;

public class SyncServiceTest extends ServiceTestCase<OdkSyncService> {

	public SyncServiceTest() {
		super(OdkSyncService.class);
	}

	public SyncServiceTest(Class<OdkSyncService> serviceClass) {
		super(serviceClass);
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

	// ///////////////////////////////////////////
	// ///////// HELPER FUNCTIONS ////////////////
	// ///////////////////////////////////////////

	private OdkSyncServiceInterface bindToService() {
		Intent bind_intent = new Intent();
		bind_intent.setClassName(SyncConsts.SYNC_SERVICE_PACKAGE,
				SyncConsts.SYNC_SERVICE_CLASS);
		IBinder service = this.bindService(bind_intent);
		OdkSyncServiceInterface odkSyncServiceInterface = OdkSyncServiceInterface.Stub
				.asInterface(service);
		return odkSyncServiceInterface;
	}

	private void assertStatusCorrect(
			OdkSyncServiceInterface syncServiceInterface,
			SyncStatus syncStatus) {
		try {
			String status = syncServiceInterface.getSyncStatus(SyncUtil.getDefaultAppName());
			assertTrue(status.equals(syncStatus.name()));
		} catch (RemoteException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
}
