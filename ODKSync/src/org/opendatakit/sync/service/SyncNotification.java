package org.opendatakit.sync.service;


import android.app.Notification;
import android.app.NotificationManager;
import android.content.Context;
import android.util.Log;

public final class SyncNotification {
  private static final String LOGTAG = SyncNotification.class.getSimpleName();

  private final Context cntxt;
  private final String appName;
  private final NotificationManager notificationManager;

  private int messageNum;
  private String updateText;
  private SyncProgressState progressState;
  private Notification.Builder builder;
  
  public SyncNotification(Context context, String appName) {
    this.cntxt = context;
    this.appName = appName;
    this.notificationManager = (NotificationManager) cntxt
        .getSystemService(Context.NOTIFICATION_SERVICE);
    this.messageNum = 0;
    this.updateText = null;
    this.progressState = null;
    this.builder = new Notification.Builder(cntxt);
  }

  public synchronized void updateNotification(SyncProgressState pgrState, String text, int maxProgress,
      int progress, boolean indeterminateProgress) {
    this.progressState = pgrState;
    this.updateText = text;
    builder.setContentTitle("ODK syncing " + appName).setContentText(text).setAutoCancel(false).setOngoing(true);
    builder.setSmallIcon(android.R.drawable.ic_popup_sync);
    builder.setProgress(maxProgress, progress, indeterminateProgress);
    Notification syncNotif = builder.getNotification();

    notificationManager.notify(appName, messageNum, syncNotif);
    Log.e(LOGTAG, messageNum + " Update SYNC Notification -" + appName + " TEXT:" + text + " PROG:"
        + progress);

  }

  public synchronized String getUpdateText() {
    return updateText;
  }

  public synchronized SyncProgressState getProgressState() {
    return progressState;
  }

  public synchronized void finalErrorNotification(String text) {
    this.progressState = SyncProgressState.ERROR;
    this.updateText = text;
    Notification.Builder finalBuilder = new Notification.Builder(cntxt);
    finalBuilder.setContentTitle("ODK SYNC ERROR " + appName).setContentText(text).setAutoCancel(true).setOngoing(false);
    finalBuilder.setSmallIcon(android.R.drawable.ic_dialog_alert);
 
    Notification syncNotif = finalBuilder.getNotification();

    notificationManager.notify(appName, messageNum, syncNotif);
    Log.e(LOGTAG, messageNum + " FINAL SYNC Notification -" + appName + " TEXT:" + text);
  }
  
  public synchronized void clearNotification() {
    notificationManager.cancel(appName, messageNum);
  }

}
