package org.opendatakit.sync.service;

import java.io.IOException;
import java.util.Arrays;

import org.opendatakit.sync.R;
import org.opendatakit.sync.SyncApp;
import org.opendatakit.sync.SyncPreferences;
import org.opendatakit.sync.SyncProcessor;
import org.opendatakit.sync.SynchronizationResult;
import org.opendatakit.sync.SynchronizationResult.Status;
import org.opendatakit.sync.Synchronizer;
import org.opendatakit.sync.TableResult;
import org.opendatakit.sync.activities.SyncActivity;
import org.opendatakit.sync.aggregate.AggregateSynchronizer;
import org.opendatakit.sync.exceptions.InvalidAuthTokenException;
import org.opendatakit.sync.exceptions.NoAppNameSpecifiedException;

import android.app.Service;
import android.content.Context;
import android.util.Log;

public class AppSynchronizer {

  private static final String LOGTAG = AppSynchronizer.class.getSimpleName();

  private final Service service;
  private final String appName;
  private final GlobalSyncNotificationManager globalNotifManager;

  private SyncStatus status;
  private Thread curThread;
  private SyncTask curTask;
  private SyncNotification syncProgress;

  AppSynchronizer(Service srvc, String appName, GlobalSyncNotificationManager notificationManager) {
    this.service = srvc;
    this.appName = appName;
    this.status = SyncStatus.INIT;
    this.curThread = null;
    this.globalNotifManager = notificationManager;
    this.syncProgress = new SyncNotification(srvc, appName);
  }

  public boolean synchronize(boolean push) {
    if (curThread == null || (!curThread.isAlive() || curThread.isInterrupted())) {
      curTask = new SyncTask(service, push);
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

  public String getSyncUpdateText() {
    return syncProgress.getUpdateText();
  }

  public SyncProgressState getProgressState() {
    return syncProgress.getProgressState();
  }

  private class SyncTask implements Runnable {

    private Context cntxt;
    private boolean push;

    public SyncTask(Context context, boolean push) {
      this.cntxt = context;
      this.push = push;
    }

    @Override
    public void run() {

      try {

        // android.os.Debug.waitForDebugger();

        globalNotifManager.startingSync(appName);
        syncProgress.updateNotification(SyncProgressState.INIT, cntxt.getString(R.string.starting_sync), 100, 0, false);
        sync(syncProgress);
        // leave the last notification available for review
        // syncProgress.clearNotification();
      } catch (NoAppNameSpecifiedException e) {
        e.printStackTrace();
        status = SyncStatus.NETWORK_ERROR;
        syncProgress.updateNotification(SyncProgressState.COMPLETE, "There were failures..." , 100, 0, false);
      } finally {
        try {
          globalNotifManager.stoppingSync(appName);
        } catch (NoAppNameSpecifiedException e) {
          // impossible to get here
        }
        SyncActivity.refreshActivityUINeeded();
      }

    }

    private void sync(SyncNotification syncProgress) {

      SyncPreferences prefs = null;
      try {
        prefs = new SyncPreferences(cntxt, appName);
      } catch (Exception e1) {
        e1.printStackTrace();
      }

      if(prefs == null) {
        status = SyncStatus.FILE_ERROR;
        syncProgress.updateNotification(SyncProgressState.INIT, "Unable to open SyncPreferences" , 100, 0, false);
        return;
      }

      try {

        Log.i(LOGTAG, "APPNAME IN SERVICE: " + appName);
        Log.i(LOGTAG, "TOKEN IN SERVICE:" + prefs.getAuthToken());
        Log.i(LOGTAG, "URI IN SEVERICE:" + prefs.getServerUri());

        // TODO: should use the APK manager to search for org.opendatakit.N
        // packages, and collect N:V strings e.g., 'survey:1', 'tables:1',
        // 'scan:1' etc. where V is the > 100's digit of the version code.
        // The javascript API and file representation are the 100's and
        // higher place in the versionCode. N is the next package in the
        // package chain.
        // TODO: Future: Add config option to specify a list of other APK
        // prefixes to the set of APKs to discover (e.g., for 3rd party
        // app support).
        //
        // NOTE: server limits this string to 10 characters
        // For now, assume all APKs are sync'd to the same API version.
        String versionCode = SyncApp.getInstance().getVersionCodeString();
        String odkClientVersion = versionCode.substring(0, versionCode.length() - 2);

        Synchronizer synchronizer = new AggregateSynchronizer(appName, odkClientVersion,
            prefs.getServerUri(), prefs.getAuthToken());
        SyncProcessor processor = new SyncProcessor(cntxt, appName, synchronizer, syncProgress);

        status = SyncStatus.SYNCING;

        // sync the app-level files, table schemas and table-level files
        processor.synchronizeConfigurationAndContent(push);

        // and now sync the data rows. This does not proceed if there
        // was an app-level sync failure or if the particular tableId
        // experienced a table-level sync failure in the preceeding step.

        processor.synchronizeDataRowsAndAttachments();

        // examine results
        SynchronizationResult overallResults = processor.getOverallResults();
        for (TableResult result : overallResults.getTableResults()) {
          org.opendatakit.sync.SynchronizationResult.Status tableStatus = result.getStatus();
          // TODO: decide how to handle the status
          if (tableStatus != Status.SUCCESS) {
            status = SyncStatus.NETWORK_ERROR;
          }
        }

        // if rows aren't successful, fail.
        if (status != SyncStatus.SYNCING) {
          syncProgress.updateNotification(SyncProgressState.COMPLETE, "There were failures..." , 100, 0, false);
          return;
        }

        // success
        status = SyncStatus.SYNC_COMPLETE;
        syncProgress.updateNotification(SyncProgressState.COMPLETE, "Successful Sync" , 100, 100, false);
        Log.i(LOGTAG, "[SyncThread] timestamp: " + System.currentTimeMillis());
      } catch (InvalidAuthTokenException e) {
        try {
          prefs.setAuthToken(null);
        } catch (IOException e1) {
          status = SyncStatus.FILE_ERROR;
          syncProgress.updateNotification(SyncProgressState.COMPLETE, "Unable to open SyncPreferences" , 100, 0, false);
          e1.printStackTrace();
        }

        syncProgress.updateNotification(SyncProgressState.COMPLETE, "Account Re-Authorization Required", 100, 0, false);
        status = SyncStatus.AUTH_RESOLUTION;
      } catch (Exception e) {
        Log.i(
            LOGTAG,
            "[exception during synchronization. stack trace:\n"
                + Arrays.toString(e.getStackTrace()));
        String msg = e.getLocalizedMessage();
        if ( msg == null ) {
          msg = e.getMessage();
        }
        if ( msg == null ) {
          msg = e.toString();
        }
        syncProgress.updateNotification(SyncProgressState.COMPLETE, "Failed Sync: " + msg , 100, 0, false);
        status = SyncStatus.NETWORK_ERROR;
      }
    }

  }
}
