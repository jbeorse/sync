package org.opendatakit.sync.service;

import java.util.HashMap;
import java.util.Map;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

public class OdkSyncService extends Service {

	private static final String LOGTAG = OdkSyncService.class.getSimpleName();

	private Map<String, AppSynchronizer> syncs;
	private OdkSyncServiceInterfaceImpl serviceInterface;

	@Override
	public void onCreate() {
		serviceInterface = new OdkSyncServiceInterfaceImpl(this);
		syncs = new HashMap<String, AppSynchronizer>();
	}

	@Override
	public IBinder onBind(Intent intent) {
		return serviceInterface;
	}

	@Override
	public void onDestroy() {
		Log.i(LOGTAG, "Service is shutting down");

	}

	private AppSynchronizer getSync(String appName) {
		AppSynchronizer sync = syncs.get(appName);
		if(sync == null) {
			sync = new AppSynchronizer(this, appName);
		}
		return sync;
		
	}
	
	public boolean push(String appName) {
		AppSynchronizer sync = getSync(appName);
		return sync.synchronize(true);
	}

   public boolean synchronize(String appName) {
		AppSynchronizer sync = getSync(appName);
		return sync.synchronize(false);
   }

	public SyncStatus getStatus(String appName) {
		AppSynchronizer sync = getSync(appName);
		return sync.getStatus();
	}

	

}
