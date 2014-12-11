package org.opendatakit.sync;

import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.utilities.CsvUtil;
import org.opendatakit.sync.SynchronizationResult.Status;
import org.opendatakit.sync.Synchronizer.SynchronizerStatus;
import org.opendatakit.sync.service.SyncNotification;
import org.opendatakit.sync.service.SyncProgressState;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SyncExecutionContext implements SynchronizerStatus {

  private static final int OVERALL_PROGRESS_BAR_LENGTH = 6350400;
  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
  }
  
  /**
   * The results of the synchronization that we will pass back to the user.
   */
  private final SynchronizationResult mUserResult;

  private int nMajorSyncSteps;
  private int iMajorSyncStep;
  private int GRAINS_PER_MAJOR_SYNC_STEP;

  private final Context context;
  private final String appName;
  private final SyncNotification syncProgress;
  private final Synchronizer synchronizer;
  private final CsvUtil utils;
  
  public SyncExecutionContext(Context context, String appName, Synchronizer synchronizer, SyncNotification syncProgress, SynchronizationResult syncResult) {
    this.context = context;
    this.appName = appName;
    this.syncProgress = syncProgress;
    this.synchronizer = synchronizer;
    this.utils = new CsvUtil(context, appName);
    this.mUserResult = syncResult;
    
    this.nMajorSyncSteps = 1;
    this.GRAINS_PER_MAJOR_SYNC_STEP = (OVERALL_PROGRESS_BAR_LENGTH / nMajorSyncSteps);
    this.iMajorSyncStep = 0;

  }

  public String getString(int resId) {
    return context.getString(resId);
  }
  
  public void setAppLevelStatus(Status status) {
    mUserResult.setAppLevelStatus(status);
  }
  
  public TableResult getTableResult(String tableId) {
    return mUserResult.getTableResult(tableId);
  }
  
  public String getAppName() {
    return this.appName;
  }
  
  public CsvUtil getCsvUtil() {
    return this.utils;
  }
  
  public Synchronizer getSynchronizer() {
    return synchronizer;
  }
  
  public SQLiteDatabase getDatabase() {
    return DatabaseFactory.get().getDatabase(context, appName);
  }

  public void resetMajorSyncSteps(int nMajorSyncSteps) {
    this.nMajorSyncSteps = nMajorSyncSteps;
    this.GRAINS_PER_MAJOR_SYNC_STEP = (OVERALL_PROGRESS_BAR_LENGTH / nMajorSyncSteps);
    this.iMajorSyncStep = 0;
  }
  
  public void incMajorSyncStep() {
    ++iMajorSyncStep;
    if ( iMajorSyncStep > nMajorSyncSteps ) {
      iMajorSyncStep = nMajorSyncSteps - 1;
    }
  }
  
  @Override
  public void updateNotification(SyncProgressState state, int textResource, Object[] formatArgVals,
      Double progressPercentage, boolean indeterminateProgress) {
    String text = "Bad text resource id: " + textResource + "!";
    String fmt = context.getString(textResource);
    if (fmt != null) {
      if (formatArgVals == null) {
        text = fmt;
      } else {
        text = String.format(fmt, formatArgVals);
      }
    }
    syncProgress.updateNotification(state, text, OVERALL_PROGRESS_BAR_LENGTH, (int) (iMajorSyncStep
        * GRAINS_PER_MAJOR_SYNC_STEP + ((progressPercentage != null) ? (progressPercentage
        * GRAINS_PER_MAJOR_SYNC_STEP / 100.0) : 0.0)), indeterminateProgress);
  }

}
