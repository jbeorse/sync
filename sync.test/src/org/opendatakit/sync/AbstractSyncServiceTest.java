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
			SyncStatus status = syncServiceInterface.getSyncStatus(SyncUtil.getDefaultAppName());
			assertTrue(status.equals(syncStatus));
		} catch (RemoteException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
}
