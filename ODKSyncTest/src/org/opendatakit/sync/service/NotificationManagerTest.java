package org.opendatakit.sync.service;

import org.opendatakit.sync.AbstractSyncServiceTest;
import org.opendatakit.sync.exceptions.NoAppNameSpecifiedException;

public class NotificationManagerTest extends AbstractSyncServiceTest {

	public NotificationManagerTest() {
		super();
	}

	public void testBasicStatusChange() {
		bindToService();
		String appName1 = "survey";
		try {

			NotificationManager manager = new NotificationManager(getService(), true);
			manager.startingSync(appName1);
			assertTrue(manager.isDisplayingNotification());
			manager.stopingSync(appName1);
			assertFalse(manager.isDisplayingNotification());
		} catch (NoAppNameSpecifiedException e) {
			e.printStackTrace();
			assertTrue(false);
		}  
		shutdownService();
	}
	
	public void testStatusChanges() {
		String appName1 = "survey";
		String appName2 = "tables";
		try {
			NotificationManager manager = new NotificationManager(getService(), true);
			manager.startingSync(appName1);
			assertTrue(manager.isDisplayingNotification());
			manager.startingSync(appName2);
			assertTrue(manager.isDisplayingNotification());
			manager.stopingSync(appName1);
			assertTrue(manager.isDisplayingNotification());
			manager.stopingSync(appName2);
			assertFalse(manager.isDisplayingNotification());
			
		} catch (NoAppNameSpecifiedException e) {
			e.printStackTrace();
			assertTrue(false);
		}  
	}

	public void testComplexStatusChanges() {
		String appName1 = "survey";
		String appName2 = "tables";
		String appName3 = "syncing";
		try {
			NotificationManager manager = new NotificationManager(getService(), true);
			manager.startingSync(appName1);
			assertTrue(manager.isDisplayingNotification());
			manager.startingSync(appName2);
			manager.startingSync(appName3);
			assertTrue(manager.isDisplayingNotification());
			manager.stopingSync(appName1);
			assertTrue(manager.isDisplayingNotification());
			manager.stopingSync(appName2);
			assertTrue(manager.isDisplayingNotification());
			manager.stopingSync(appName3);
			assertFalse(manager.isDisplayingNotification());
			manager.startingSync(appName2);
			manager.startingSync(appName3);
			assertTrue(manager.isDisplayingNotification());
			manager.stopingSync(appName1);
			assertTrue(manager.isDisplayingNotification());
			manager.stopingSync(appName2);
			manager.stopingSync(appName3);
			assertFalse(manager.isDisplayingNotification());
			
		} catch (NoAppNameSpecifiedException e) {
			e.printStackTrace();
			assertTrue(false);
		}  
	}
	
	public void testNull() {
		
		try {
			NotificationManager manager = new NotificationManager(getService());
			manager.startingSync(null);
			assertTrue(false);
			
			
		} catch (NoAppNameSpecifiedException e) {
			assertTrue(true);
		}  
	}
}
