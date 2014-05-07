package org.opendatakit.sync;

import java.util.concurrent.atomic.AtomicBoolean;

import org.opendatakit.sync.service.OdkSyncService;
import org.opendatakit.sync.service.OdkSyncServiceInterface;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;

public class OdkSyncServiceProxy implements ServiceConnection {

	private final static String TAG = "OdkSyncServiceProxy";

	private OdkSyncServiceInterface sensorSvcProxy;

	protected Context componentContext;
	protected final AtomicBoolean isBoundToService = new AtomicBoolean(false);

	public OdkSyncServiceProxy(Context context) {
		this(context, OdkSyncService.BENCHMARK_SERVICE_PACKAGE,
				OdkSyncService.BENCHMARK_SERVICE_CLASS);
	}

	public OdkSyncServiceProxy(Context context, String frameworkPackage,
			String frameworkService) {
		componentContext = context;
		Intent bind_intent = new Intent();
		bind_intent.setClassName(frameworkPackage, frameworkService);
		componentContext.bindService(bind_intent, this,
				Context.BIND_AUTO_CREATE);
	}

	public void shutdown() {
		Log.d(TAG, "Application shutdown - unbinding from Syncervice");
		if (isBoundToService.get()) {
			try {
				componentContext.unbindService(this);
				isBoundToService.set(false);
				Log.d(TAG, "unbound to sensor service");
			} catch (Exception ex) {
				Log.d(TAG, "onDestroy threw exception");
				ex.printStackTrace();
			}
		}
	}

	@Override
	public void onServiceConnected(ComponentName className, IBinder service) {
		Log.d(TAG, "Bound to sensor service");
		sensorSvcProxy = OdkSyncServiceInterface.Stub.asInterface(service);
		isBoundToService.set(true);
	}

	@Override
	public void onServiceDisconnected(ComponentName arg0) {
		Log.d(TAG, "unbound to sensor service");
		isBoundToService.set(false);
	}

	public String getSyncStatus() throws RemoteException {
		try {
			return sensorSvcProxy.getSyncStatus();
		} catch (RemoteException rex) {
			rex.printStackTrace();
			throw rex;
		}
	}

	public boolean synchronize() throws RemoteException {
		try {
			return sensorSvcProxy.synchronize();
		} catch (RemoteException rex) {
			rex.printStackTrace();
			throw rex;
		}
	}

	public boolean pushToServer() throws RemoteException {
		try {
			return sensorSvcProxy.push();
		} catch (RemoteException rex) {
			rex.printStackTrace();
			throw rex;
		}
	}
	
	public boolean isBoundToService() {
		return isBoundToService.get();
	}

}
