package org.opendatakit.sync.service;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;

public class OdkSyncService extends Service {

	private OdkSyncServiceInterfaceImpl serviceInterface;
	
	@Override
	public void onCreate() {
		serviceInterface = new OdkSyncServiceInterfaceImpl(this);
	}

	
	@Override
	public IBinder onBind(Intent intent) {
		// TODO Auto-generated method stub
		return null;
	}

}
