package org.opendatakit.sync.service;

import java.util.Arrays;

import org.opendatakit.sync.SyncPreferences;
import org.opendatakit.sync.SyncProcessor;
import org.opendatakit.sync.SynchronizationResult;
import org.opendatakit.sync.Synchronizer;
import org.opendatakit.sync.TableResult;
import org.opendatakit.sync.aggregate.AggregateSynchronizer;
import org.opendatakit.sync.exceptions.InvalidAuthTokenException;

import android.content.Context;
import android.content.SyncResult;
import android.util.Log;

public class AppSynchronizer {

	private static final String LOGTAG = AppSynchronizer.class.getSimpleName();

	private final Context cntxt;
	private final String appName;
	
	private SyncStatus status;
	private Thread curThread;
	private SyncTask curTask;

	AppSynchronizer(Context context, String appName) {
		this.cntxt = context;
		this.appName = appName;
		this.status = SyncStatus.INIT;
		this.curThread = null;
	}

	public boolean synchronize(boolean push) {
		if(curThread == null || (!curThread.isAlive() || curThread.isInterrupted())) {
			curTask = new SyncTask(cntxt, push);
			curThread = new Thread(curTask);
			status = SyncStatus.SYNCING;
			curThread.start();
			return true;
		}
		return false;
	}

	public SyncStatus getStatus() {
		return status;
	}

	private class SyncTask implements Runnable {

		private Context cntxt;
		private boolean push;

		public SyncTask(Context context, boolean push) {
			this.cntxt = context;
			this.push = false;
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
}
