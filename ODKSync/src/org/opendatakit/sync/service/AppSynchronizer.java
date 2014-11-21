/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.sync.service;

import java.util.Arrays;

import org.opendatakit.common.android.utilities.WebLogger;
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
        syncProgress.updateNotification(SyncProgressState.STARTING,
            cntxt.getString(R.string.starting_sync), 100, 0, false);
        sync(syncProgress);
      } catch (NoAppNameSpecifiedException e) {
        WebLogger.getLogger(appName).printStackTrace(e);
        status = SyncStatus.NETWORK_ERROR;
        syncProgress.updateNotification(SyncProgressState.ERROR, "There were failures...", 100, 0,
            false);
      } finally {
        SyncActivity.refreshActivityUINeeded(appName);
        try {
          globalNotifManager.stoppingSync(appName);
        } catch (NoAppNameSpecifiedException e) {
          // impossible to get here
        }
      }

    }

    private void sync(SyncNotification syncProgress) {

      SyncPreferences prefs = null;
      try {
        prefs = new SyncPreferences(cntxt, appName);
      } catch (Exception e1) {
        WebLogger.getLogger(appName).printStackTrace(e1);
      }

      if (prefs == null) {
        status = SyncStatus.FILE_ERROR;
        syncProgress.finalErrorNotification("Unable to open SyncPreferences");
        return;
      }

      try {

        WebLogger.getLogger(appName).i(LOGTAG, "APPNAME IN SERVICE: " + appName);
        WebLogger.getLogger(appName).i(LOGTAG, "TOKEN IN SERVICE:" + prefs.getAuthToken());
        WebLogger.getLogger(appName).i(LOGTAG, "URI IN SEVERICE:" + prefs.getServerUri());

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
        // android.os.Debug.waitForDebugger();
        String odkClientVersion = versionCode.substring(0, versionCode.length() - 2);

        Synchronizer synchronizer = new AggregateSynchronizer(cntxt, appName, odkClientVersion,
            prefs.getServerUri(), prefs.getAuthToken());
        SyncProcessor processor = new SyncProcessor(cntxt, appName, synchronizer, syncProgress);

        status = SyncStatus.SYNCING;

        // sync the app-level files, table schemas and table-level files
        processor.synchronizeConfigurationAndContent(push);

        // and now sync the data rows. This does not proceed if there
        // was an app-level sync failure or if the particular tableId
        // experienced a table-level sync failure in the preceeding step.

        processor.synchronizeDataRowsAndAttachments();

        boolean authProblems = false;

        String reason = "none";
        // examine results
        SynchronizationResult overallResults = processor.getOverallResults();
        if (overallResults.getAppLevelStatus() != Status.SUCCESS) {
          authProblems = (overallResults.getAppLevelStatus() == Status.AUTH_EXCEPTION);
          reason = "overall results";
          status = SyncStatus.NETWORK_ERROR;
        }

        for (TableResult result : overallResults.getTableResults()) {
          org.opendatakit.sync.SynchronizationResult.Status tableStatus = result.getStatus();
          // TODO: decide how to handle the status
          if (tableStatus != Status.SUCCESS) {
            if (tableStatus == Status.AUTH_EXCEPTION) {
              authProblems = true;
            } else if (tableStatus == Status.TABLE_PENDING_ATTACHMENTS) {
              continue;
            } else if (tableStatus == Status.TABLE_CONTAINS_CHECKPOINTS
                || tableStatus == Status.TABLE_CONTAINS_CONFLICTS
                || tableStatus == Status.TABLE_REQUIRES_APP_LEVEL_SYNC) {
              status = SyncStatus.CONFLICT_RESOLUTION;
            } else {
              status = SyncStatus.NETWORK_ERROR;
              reason = "table " + result.getTableDisplayName();
            }
          }
        }

        if (authProblems) {
          throw new InvalidAuthTokenException("Synthesized");
        }

        // if rows aren't successful, fail.
        if (status != SyncStatus.SYNCING && status != SyncStatus.CONFLICT_RESOLUTION) {
          syncProgress.finalErrorNotification("There were failures. Status: " + status + " Reason:"
              + reason);
          return;
        }

        // success
        if (status != SyncStatus.CONFLICT_RESOLUTION) {
          status = SyncStatus.SYNC_COMPLETE;
          syncProgress.clearNotification();
        } else {
          syncProgress.finalErrorNotification("Conflicts exist.  Please resolve.");
        }

        WebLogger.getLogger(appName).i(LOGTAG,
            "[SyncThread] timestamp: " + System.currentTimeMillis());
      } catch (InvalidAuthTokenException e) {
        SyncActivity.invalidateAuthToken(cntxt, appName);
        status = SyncStatus.AUTH_RESOLUTION;
        syncProgress.finalErrorNotification("Account Re-Authorization Required");
      } catch (Exception e) {
        WebLogger.getLogger(appName).i(
            LOGTAG,
            "[exception during synchronization. stack trace:\n"
                + Arrays.toString(e.getStackTrace()));
        String msg = e.getLocalizedMessage();
        if (msg == null) {
          msg = e.getMessage();
        }
        if (msg == null) {
          msg = e.toString();
        }
        status = SyncStatus.NETWORK_ERROR;
        syncProgress.finalErrorNotification("Failed Sync: " + msg);
      }
    }

  }
}
