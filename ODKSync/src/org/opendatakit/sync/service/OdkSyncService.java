package org.opendatakit.sync.service;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

public class OdkSyncService extends Service {

	private static final String LOGTAG = "OdkSyncService";
	
	public static final String BENCHMARK_SERVICE_PACKAGE = "org.opendatakit.sync";

	public static final String BENCHMARK_SERVICE_CLASS = "org.opendatakit.sync.service.OdkSyncService";

	private SyncStatus status;
	
	private OdkSyncServiceInterfaceImpl serviceInterface;
	
	@Override
	public void onCreate() {
		serviceInterface = new OdkSyncServiceInterfaceImpl(this);
		status = SyncStatus.INIT;
	}

	

	@Override
	public int onStartCommand(Intent intent, int flags, int startId) {
		// actions if received a start command
		Log.i(LOGTAG, "Service is starting");
	
		return START_STICKY;
	}
		

	
	@Override
	public IBinder onBind(Intent intent) {
		return serviceInterface;
	}

	
	@Override
	public void onDestroy() {
		Log.i(LOGTAG, "Service is shutting down");
		
	}

	public void sync() {
		status = SyncStatus.FINISHED;
	}
	
	public SyncStatus getStatus() {		
		return status;
	}
}
