package org.opendatakit.sync.service;

import org.opendatakit.sync.activities.SyncActivity;

import android.R;
import android.app.Notification;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.util.Log;

public final class SyncNotification {
  private static final String LOGTAG = SyncNotification.class.getSimpleName();

  private final Context cntxt;
  private final String appName;
  private final NotificationManager notificationManager;
  private int messageNum;

  public SyncNotification(Context context, String appName) {
    this.cntxt = context;
    this.appName = appName;
    this.notificationManager = (NotificationManager) cntxt
        .getSystemService(Context.NOTIFICATION_SERVICE);
    this.messageNum = 0;
  }

  public synchronized void updateNotification(String text, int maxProgress, int progress,
      boolean indeterminateProgress) {
    Notification.Builder builder = new Notification.Builder(cntxt);
    builder.setContentTitle("ODK syncing " + appName).setContentText(text).setAutoCancel(false).setOngoing(true);
    builder.setSmallIcon(R.drawable.ic_popup_sync);
    builder.setProgress(maxProgress, progress, indeterminateProgress);

    Notification syncNotif = builder.getNotification();
    // syncNotif.flags |= Notification.FLAG_NO_CLEAR;

    notificationManager.notify(appName, ++messageNum, syncNotif);
    Log.e(LOGTAG, messageNum + " Update SYNC Notification -" + appName + " TEXT:" + text + " PROG:"
        + progress);

  }

  public synchronized void clearNotification() {
    notificationManager.cancel(appName, messageNum);
  }

}
