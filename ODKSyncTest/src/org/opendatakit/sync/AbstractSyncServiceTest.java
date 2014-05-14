package org.opendatakit.sync;

import org.opendatakit.sync.files.SyncUtil;
import org.opendatakit.sync.service.OdkSyncService;
import org.opendatakit.sync.service.OdkSyncServiceInterface;
import org.opendatakit.sync.service.SyncStatus;

import android.content.Intent;
import android.os.IBinder;
import android.os.RemoteException;
import android.test.ServiceTestCase;

public abstract class AbstractSyncServiceTest extends ServiceTestCase<OdkSyncService> {

	protected AbstractSyncServiceTest() {
		super(OdkSyncService.class);
	}

	protected AbstractSyncServiceTest(Class<OdkSyncService> serviceClass) {
		super(serviceClass);
	}
	
	@Override
	protected void setUp () throws Exception {
		super.setUp();
		setupService();		
	}

	// ///////////////////////////////////////////
	// ///////// HELPER FUNCTIONS ////////////////
	// ///////////////////////////////////////////

	protected OdkSyncServiceInterface bindToService() {
		Intent bind_intent = new Intent();
		bind_intent.setClassName(SyncConsts.SYNC_SERVICE_PACKAGE,
				SyncConsts.SYNC_SERVICE_CLASS);
		IBinder service = this.bindService(bind_intent);
		OdkSyncServiceInterface odkSyncServiceInterface = OdkSyncServiceInterface.Stub
				.asInterface(service);
		return odkSyncServiceInterface;
	}

	protected void assertStatusCorrect(
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
