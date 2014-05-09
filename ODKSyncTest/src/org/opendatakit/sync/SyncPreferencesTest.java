package org.opendatakit.sync;

import java.io.IOException;

import org.opendatakit.sync.exceptions.NoAppNameSpecifiedException;
import org.opendatakit.sync.service.OdkSyncService;

import android.test.ServiceTestCase;

public class SyncPreferencesTest extends ServiceTestCase<OdkSyncService> {

	public SyncPreferencesTest() {
		super(OdkSyncService.class);
	}

	public SyncPreferencesTest(Class<OdkSyncService> serviceClass) {
		super(serviceClass);
	}
	
	public void testBasicParamChange() {
		String testAccount1 = "SYNC_ACCOUNT";
		String testAccount2 = "TABLES_ACCOUNT";
		try {
			SyncPreferences prefs = new SyncPreferences(getSystemContext());
			prefs.setAccount(testAccount1);
			assertTrue(testAccount1.equals(prefs.getAccount()));
			prefs.setAccount(testAccount2);
			assertTrue(testAccount2.equals(prefs.getAccount()));
			SyncPreferences prefs2 = new SyncPreferences(getSystemContext());
			assertFalse(testAccount1.equals(prefs.getAccount()));
			assertTrue(testAccount2.equals(prefs.getAccount()));
		} catch (NoAppNameSpecifiedException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(false);
		}  
	}
	
	
}
