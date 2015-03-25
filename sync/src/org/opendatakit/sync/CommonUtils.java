/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

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
