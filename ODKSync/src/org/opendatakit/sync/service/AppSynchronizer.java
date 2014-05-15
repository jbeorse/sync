package org.opendatakit.sync.service;

import java.util.Arrays;

import org.opendatakit.sync.SyncApp;
import org.opendatakit.sync.SyncPreferences;
import org.opendatakit.sync.SyncProcessor;
import org.opendatakit.sync.SynchronizationResult;
import org.opendatakit.sync.Synchronizer;
import org.opendatakit.sync.TableResult;
import org.opendatakit.sync.TableResult.Status;
import org.opendatakit.sync.aggregate.AggregateSynchronizer;
import org.opendatakit.sync.exceptions.InvalidAuthTokenException;
import org.opendatakit.sync.exceptions.NoAppNameSpecifiedException;

import android.app.Service;
import android.content.Context;
import android.content.SyncResult;
import android.util.Log;

public class AppSynchronizer {

  private static final String LOGTAG = AppSynchronizer.class.getSimpleName();

  private final Service service;
  private final String appName;
  private final GlobalSyncNotificationManager globalNotifManager;

  private SyncStatus status;
  private Thread curThread;
  private SyncTask curTask;

  AppSynchronizer(Service context, String appName, GlobalSyncNotificationManager notificationManager) {
    this.service = context;
    this.appName = appName;
    this.status = SyncStatus.INIT;
    this.curThread = null;
    this.globalNotifManager = notificationManager;
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
        SyncNotification syncNotif = new SyncNotification(cntxt, appName);
        globalNotifManager.startingSync(appName);
        syncNotif.updateNotification("Staring to Sync", 100, 0, false);
        sync(syncNotif);
        syncNotif.clearNotification();
        globalNotifManager.stopingSync(appName);

      } catch (NoAppNameSpecifiedException e) {
        e.printStackTrace();
        status = SyncStatus.NETWORK_ERROR;
      }

    }

    private void sync(SyncNotification syncNotif) {
      try {
        SyncPreferences prefs = new SyncPreferences(cntxt, appName);
        Log.e(LOGTAG, "APPNAME IN SERVICE: " + appName);
        Log.e(LOGTAG, "TOKEN IN SERVICE:" + prefs.getAuthToken());
        Log.e(LOGTAG, "URI IN SEVERICE:" + prefs.getServerUri());

        // TODO: this should probably come in from the client???
        String versionCode = SyncApp.getInstance().getVersionCodeString();
        // the javascript API and file representation are the 100's and
        // higher place in the versionCode.
        String odkClientVersion = versionCode.substring(0, versionCode.length() - 2);

        Synchronizer synchronizer = new AggregateSynchronizer(appName, odkClientVersion,
            prefs.getServerUri(), prefs.getAuthToken());
        SyncProcessor processor = new SyncProcessor(cntxt, appName, synchronizer, new SyncResult());

        status = SyncStatus.SYNCING;

        // sync the app-level files, table schemas and table-level files
        SynchronizationResult configResults = processor.synchronizeConfigurationAndContent(push);
        // examine results
        for (TableResult result : configResults.getTableResults()) {
          TableResult.Status tableStatus = result.getStatus();
          // TODO: decide how to handle the status
          if (tableStatus != Status.SUCCESS) {
            status = SyncStatus.NETWORK_ERROR;
          }
        }

        // if the app isn't configured, fail
        if (status != SyncStatus.SYNCING) {
          return;
        }

        // TODO: should probably return to app to re-scan
        // initialization??
        // or maybe there isn't anything more to do?

        // now sync the data rows and attachments
        SynchronizationResult dataResults = processor.synchronizeDataRowsAndAttachments();
        for (TableResult result : dataResults.getTableResults()) {
          TableResult.Status tableStatus = result.getStatus();
          // TODO: decide how to handle the status
          if (tableStatus != Status.SUCCESS) {
            status = SyncStatus.NETWORK_ERROR;
          }
        }

        // if rows aren't successful, fail.
        if (status != SyncStatus.SYNCING) {
          return;
        }

        // success
        status = SyncStatus.SYNC_COMPLETE;
        Log.e(LOGTAG, "[SyncThread] timestamp: " + System.currentTimeMillis());
      } catch (InvalidAuthTokenException e) {
        status = SyncStatus.AUTH_RESOLUTION;
      } catch (Exception e) {
        Log.e(
            LOGTAG,
            "[exception during synchronization. stack trace:\n"
                + Arrays.toString(e.getStackTrace()));
        status = SyncStatus.NETWORK_ERROR;
      }
    }

  }
}
