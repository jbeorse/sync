package org.opendatakit.sync.service;

import java.util.ArrayList;
import java.util.List;

import org.opendatakit.sync.R;
import org.opendatakit.sync.exceptions.NoAppNameSpecifiedException;

import android.app.Notification;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;

public final class GlobalSyncNotificationManager {

	private static final int UNIQUE_ID = 1337;

	private final Service service;
	private final boolean test;

	private boolean displayNotification;

	private List<AppSyncStatus> statusList;

	GlobalSyncNotificationManager(Service service) {
		this.test = false;
		this.service = service;
		this.displayNotification = false;
		this.statusList = new ArrayList<AppSyncStatus>();
	}

	GlobalSyncNotificationManager(Service service, boolean test) {
		this.test = test;
		this.service = service;
		this.displayNotification = false;
		this.statusList = new ArrayList<AppSyncStatus>();
	}

	public synchronized void startingSync(String appName)
			throws NoAppNameSpecifiedException {
		AppSyncStatus appStatus = getAppStatus(appName);
		appStatus.setSyncing(true);
		update();
	}

	public synchronized void stoppingSync(String appName)
			throws NoAppNameSpecifiedException {
		AppSyncStatus appStatus = getAppStatus(appName);
		appStatus.setSyncing(false);
		update();
	}

	public synchronized boolean isDisplayingNotification() {
		return displayNotification;
	}

	private AppSyncStatus getAppStatus(String appName)
			throws NoAppNameSpecifiedException {
		if (appName == null) {
			throw new NoAppNameSpecifiedException(
					"Cannot update NotificationManager without appName");
		}

		AppSyncStatus appStatus = null;

		// see if manager already knows about the app
		for (AppSyncStatus status : statusList) {
			if (status.getAppName().equals(appName)) {
				appStatus = status;
			}
		}

		// if manager does not know about app, create it
		if (appStatus == null) {
			appStatus = new AppSyncStatus(appName);
			statusList.add(appStatus);
		}
		return appStatus;
	}

	private void update() {
		// check if NotificationManager should be displaying notification
		boolean shouldDisplay = false;
		for (AppSyncStatus status : statusList) {
			if (status.isSyncing()) {
				shouldDisplay = true;
			}
		}

		// if should and actual do not match fix
		if (shouldDisplay && !displayNotification) {
			createNotification();
		} else if (!shouldDisplay && displayNotification) {
			removeNotification();
		}

		assert (shouldDisplay == displayNotification);
	}

	private void createNotification() {
		// The intent to launch when the user clicks the expanded notification
//		Intent tmpIntent = new Intent(service, SyncActivity.class);
	   Intent tmpIntent = new Intent(Intent.ACTION_VIEW);
	   tmpIntent.setClassName("org.opendatakit.sync", "org.opendatakit.sync.activities.SyncActivity");
		tmpIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
		PendingIntent pendIntent = PendingIntent.getActivity(service.getApplicationContext(), 0,
				tmpIntent, 0);

		Notification.Builder builder = new Notification.Builder(service);
		builder.setTicker("ODK Syncing").setContentTitle("ODK Sync")
				.setContentText("ODK is syncing an Application")
				.setWhen(System.currentTimeMillis()).setAutoCancel(false)
				.setOngoing(true).setContentIntent(pendIntent).setSmallIcon(R.drawable.ic_launcher);

		Notification runningNotification = builder.getNotification();
		runningNotification.flags |= Notification.FLAG_NO_CLEAR;

		if (!test) {
			service.startForeground(UNIQUE_ID, runningNotification);
		}
		displayNotification = true;
	}

	private void removeNotification() {
		if (!test) {
			service.stopForeground(true);
		}
		displayNotification = false;
	}

	private final class AppSyncStatus {
		private final String appName;
		private boolean syncing;

		AppSyncStatus(String appName) {
			this.appName = appName;
			this.syncing = false;
		}

		public String getAppName() {
			return appName;
		}

		public void setSyncing(boolean syncing) {
			this.syncing = syncing;
		}

		public boolean isSyncing() {
			return syncing;
		}

	}
}
