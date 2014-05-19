package org.opendatakit.sync.service;

import org.opendatakit.sync.service.OdkSyncServiceInterface.Stub;

import android.os.RemoteException;
import android.util.Log;

public class OdkSyncServiceInterfaceImpl extends Stub {

	private static final String LOGTAG = OdkSyncServiceInterfaceImpl.class
			.getSimpleName();
	private OdkSyncService syncService;

	public OdkSyncServiceInterfaceImpl(OdkSyncService syncService) {
		this.syncService = syncService;
	}

	@Override
	public SyncStatus getSyncStatus(String appName) throws RemoteException {
		try {
			Log.i(LOGTAG, "SERVICE INTERFACE: getSyncStatus WITH appName:"
					+ appName);
			return syncService.getStatus(appName);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
			throw new RemoteException();
		}
	}

	@Override
	public boolean push(String appName) throws RemoteException {
		try {
			Log.i(LOGTAG, "SERVICE INTERFACE: push WITH appName:" + appName);
			return syncService.push(appName);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
			throw new RemoteException();
		}
	}

	@Override
	public boolean synchronize(String appName) throws RemoteException {
		try {
			Log.i(LOGTAG, "SERVICE INTERFACE: synchronize WITH appName:"
					+ appName);
			return syncService.synchronize(appName);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
			throw new RemoteException();
		}
	}
	
	@Override
	public SyncProgressState getSyncProgress(String appName) throws RemoteException {
	  try {
       Log.i(LOGTAG, "SERVICE INTERFACE: getSyncProgress WITH appName:"
             + appName);
       return syncService.getSyncProgress(appName);
    } catch (Throwable throwable) {
       throwable.printStackTrace();
       throw new RemoteException();
    }
	}
   
	@Override
   public String getSyncUpdateMessage(String appName) throws RemoteException {
	  try {
       Log.i(LOGTAG, "SERVICE INTERFACE: getSyncUpdateMessage WITH appName:"
             + appName);
       return syncService.getSyncUpdateMessage(appName);
    } catch (Throwable throwable) {
       throwable.printStackTrace();
       throw new RemoteException();
    }
	}
}
