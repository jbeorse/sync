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
	private GlobalSyncNotificationManager notificationManager;
	
	
	@Override
	public void onCreate() {
		serviceInterface = new OdkSyncServiceInterfaceImpl(this);
		syncs = new HashMap<String, AppSynchronizer>();
		notificationManager = new GlobalSyncNotificationManager(this);
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
			sync = new AppSynchronizer(this, appName,notificationManager);
			syncs.put(appName, sync);
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

  
   public SyncProgressState getSyncProgress(String appName){
     // TODO: PIPE IN REAL IMPLEMENTATION
     return SyncProgressState.FILES;
   }
   
  
   public String getSyncUpdateMessage(String appName) {
     // TODO: PIPE IN REAL IMPLEMENTATION
     return "NEED TO IMPLEMENT";
   }

	
}
