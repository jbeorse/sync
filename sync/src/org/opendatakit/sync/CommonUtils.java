package org.opendatakit.sync;

import org.opendatakit.aggregate.odktables.rest.KeyValueStoreConstants;
import org.opendatakit.common.android.utilities.KeyValueStoreHelper;
import org.opendatakit.common.android.utilities.NameUtil;
import org.opendatakit.common.android.utilities.ODKDataUtils;
import org.opendatakit.common.android.utilities.KeyValueStoreHelper.AspectHelper;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.sync.application.Sync;

import android.os.RemoteException;

public class CommonUtils {
  
  public static String getLocalizedDisplayName(String appName, OdkDbHandle db, String tableId) throws RemoteException {
    
    KeyValueStoreHelper kvsh = new KeyValueStoreHelper(Sync.getInstance(), appName, db, tableId, KeyValueStoreConstants.PARTITION_TABLE);
    AspectHelper ah = kvsh.getAspectHelper(KeyValueStoreConstants.ASPECT_DEFAULT);
    String displayName = null;
    String jsonDisplayName = ah.getObject(KeyValueStoreConstants.TABLE_DISPLAY_NAME);
    if ( jsonDisplayName != null ) {
      displayName = ODKDataUtils.getLocalizedDisplayName(jsonDisplayName);
    }
    if ( displayName == null ) {
      displayName = NameUtil.constructSimpleDisplayName(tableId);
    }
    return displayName;
  }

}
