package org.opendatakit.sync.service;

import org.opendatakit.sync.service.OdkSyncServiceInterface.Stub;

import android.os.RemoteException;

public class OdkSyncServiceInterfaceImpl extends Stub {

	private OdkSyncService syncService;

	public OdkSyncServiceInterfaceImpl(OdkSyncService syncService) {
		this.syncService = syncService;
	}
	
	@Override
	public String getSyncStatus() throws RemoteException {
		return syncService.getStatus().toString();
	}

	@Override
	public boolean synchronize() throws RemoteException {
		syncService.sync();
		return true;
	}

	@Override
	public boolean push() throws RemoteException {
		syncService.push();
		return true;
	}

}
