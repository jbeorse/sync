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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean synchronize() throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}

}
