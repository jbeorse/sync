package org.opendatakit.sync.service;

interface OdkSyncServiceInterface {

	String getSyncStatus();
	
	boolean synchronize();

}
