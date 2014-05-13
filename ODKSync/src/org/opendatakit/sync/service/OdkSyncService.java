package org.opendatakit.sync.service;

import java.util.Arrays;

import org.opendatakit.sync.SyncPreferences;
import org.opendatakit.sync.SyncProcessor;
import org.opendatakit.sync.SynchronizationResult;
import org.opendatakit.sync.Synchronizer;
import org.opendatakit.sync.TableResult;
import org.opendatakit.sync.aggregate.AggregateSynchronizer;
import org.opendatakit.sync.exceptions.InvalidAuthTokenException;
import org.opendatakit.sync.files.SyncUtil;

import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.SyncResult;
import android.os.IBinder;
import android.util.Log;

public class OdkSyncService extends Service {

	private static final String LOGTAG = "OdkSyncService";

	private SyncStatus status;
	private String syncAppName;
	
	private SyncThread syncThread;
	private OdkSyncServiceInterfaceImpl serviceInterface;

	@Override
	public void onCreate() {
		serviceInterface = new OdkSyncServiceInterfaceImpl(this);
		status = SyncStatus.INIT;	
		syncAppName = null;
		syncThread = new SyncThread(this);
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

	public void push() {
//		SyncNowTask syncTask = new SyncNowTask(this, appName, true);
//		syncTask.execute();

		 if (!syncThread.isAlive() || syncThread.isInterrupted()) {
		 syncThread.setPush(true);
		 status = SyncStatus.SYNCING;
		 syncThread.start();
		 }

	}

   public void synchronize() {
//    SyncNowTask syncTask = new SyncNowTask(this, appName, true);
//    syncTask.execute();

       if (!syncThread.isAlive() || syncThread.isInterrupted()) {
       syncThread.setPush(false);
       status = SyncStatus.SYNCING;
       syncThread.start();
       }

   }

	public SyncStatus getStatus() {
		return status;
	}

	// TEMPORARY THREAD while transition API
	private class SyncThread extends Thread {

		private Context cntxt;

		private boolean push;

		public SyncThread(Context context) {
			this.cntxt = context;
			this.push = false;
		}

		public void setPush(boolean push) {
			this.push = push;
		}

		@Override
		public void run() {
			try {
				
				String appName;
				if(syncAppName == null){
					appName = SyncUtil.getDefaultAppName();
				} else {
					appName = syncAppName;
				}
				
				SyncPreferences prefs = new SyncPreferences(cntxt, appName);
				Log.e(LOGTAG, "APPNAME IN SERVICE: " + appName);
				Log.e(LOGTAG, "TOKEN IN SERVICE:" + prefs.getAuthToken());
				Log.e(LOGTAG, "URI IN SEVERICE:" + prefs.getServerUri());
				Synchronizer synchronizer = new AggregateSynchronizer(appName,
						prefs.getServerUri(), prefs.getAuthToken());
				SyncProcessor processor = new SyncProcessor(cntxt, appName,
						synchronizer, new SyncResult());

				SynchronizationResult results = processor.synchronize(push,
						push, true);

				// default to sync complete
				status = SyncStatus.SYNC_COMPLETE;
				for (TableResult result : results.getTableResults()) {
					TableResult.Status status = result.getStatus();
					// TODO: decide how to handle the status
				}

				Log.e(LOGTAG,
						"[SyncThread] timestamp: " + System.currentTimeMillis());
			} catch (InvalidAuthTokenException e) {
				status = SyncStatus.AUTH_RESOLUTION;
			} catch (Exception e) {
				Log.e(LOGTAG,
						"[exception during synchronization. stack trace:\n"
								+ Arrays.toString(e.getStackTrace()));
				status = SyncStatus.NETWORK_ERROR;
			}
		}

	}

}
