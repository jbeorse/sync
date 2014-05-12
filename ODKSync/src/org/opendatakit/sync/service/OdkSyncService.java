package org.opendatakit.sync.service;

import java.util.Arrays;

import org.opendatakit.sync.SyncPreferences;
import org.opendatakit.sync.SyncProcessor;
import org.opendatakit.sync.SynchronizationResult;
import org.opendatakit.sync.Synchronizer;
import org.opendatakit.sync.TableFileUtils;
import org.opendatakit.sync.TableResult;
import org.opendatakit.sync.aggregate.AggregateSynchronizer;
import org.opendatakit.sync.exceptions.InvalidAuthTokenException;

import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.SyncResult;
import android.os.AsyncTask;
import android.os.IBinder;
import android.util.Log;

public class OdkSyncService extends Service {

	private static final String LOGTAG = "OdkSyncService";

	public static final String BENCHMARK_SERVICE_PACKAGE = "org.opendatakit.sync";

	public static final String BENCHMARK_SERVICE_CLASS = "org.opendatakit.sync.service.OdkSyncService";

	private SyncStatus status;

	private String appName;
	private SyncThread syncThread;
	private OdkSyncServiceInterfaceImpl serviceInterface;

	@Override
	public void onCreate() {
		serviceInterface = new OdkSyncServiceInterfaceImpl(this);
		status = SyncStatus.INIT;

		if (appName == null) {
			appName = TableFileUtils.getDefaultAppName();
		}
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

	public void sync() {
		SyncNowTask syncTask = new SyncNowTask(this, appName, false);
		syncTask.execute();

		// if (!syncThread.isAlive() || syncThread.isInterrupted()) {
		// syncThread.setPush(false);
		// status = SyncStatus.SYNCING;
		// syncThread.start();
		// }
	}

	public void push() {
		SyncNowTask syncTask = new SyncNowTask(this, appName, true);
		syncTask.execute();

		// if (!syncThread.isAlive() || syncThread.isInterrupted()) {
		// syncThread.setPush(true);
		// status = SyncStatus.SYNCING;
		// syncThread.start();
		// }

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

	public class SyncNowTask extends AsyncTask<Void, Void, Void> {

		private static final String t = "SyncNowTask";

		private final Context context;
		private final String appName;
		private final boolean pushToServer;

		public SyncNowTask(Context context, String appName, boolean pushToServer) {
			this.context = context;
			this.appName = appName;
			this.pushToServer = pushToServer;
		}

		@Override
		protected Void doInBackground(Void... params) {

			try {
				SyncPreferences prefs = new SyncPreferences(context, appName);
				Log.e(t, "APPNAME IN TASK: " + appName);
				Log.e(t, "TOKEN IN TASK:" + prefs.getAuthToken());
				Log.e(t, "URI IN TASK:" + prefs.getServerUri());
				Synchronizer synchronizer = new AggregateSynchronizer(appName,
						prefs.getServerUri(), prefs.getAuthToken());
				SyncProcessor processor = new SyncProcessor(context, appName,
						synchronizer, new SyncResult());
				// The user should specify whether it is a push or a pull during
				// a sync. We always sync the data.
				SynchronizationResult results = processor.synchronize(
						pushToServer, pushToServer, true);

				// default to sync complete
				status = SyncStatus.SYNC_COMPLETE;
				for (TableResult result : results.getTableResults()) {
					TableResult.Status status = result.getStatus();
					// TODO: decide how to handle the status
				}

				Log.e(t,
						"[SyncNowTask#doInBackground] timestamp: "
								+ System.currentTimeMillis());
			} catch (InvalidAuthTokenException e) {
				status = SyncStatus.AUTH_RESOLUTION;
			} catch (Exception e) {
				Log.e(t, "[exception during synchronization. stack trace:\n"
						+ Arrays.toString(e.getStackTrace()));
				status = SyncStatus.NETWORK_ERROR;
			}
			return null;

		}

	}

}
