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

package org.opendatakit.sync.application;

import org.opendatakit.common.android.application.CommonApplication;
import org.opendatakit.sync.OdkSyncServiceProxy;
import org.opendatakit.sync.R;

public class Sync extends CommonApplication {

  public static final String LOGTAG = Sync.class.getSimpleName();

  /**
   * Set this to true if you want to attach a debugger to any or all 
   * of the services. The debugger is attached in the onBind() call.
   * This affects the OdkDbShimService, OdkWebkitServerService and
   * Sync services.
   * If true, then tables on server are dropped if not present on device.
   */
  private boolean debugService = false;
  
  private static Sync singleton = null;

  public static Sync getInstance() {
    return singleton;
  }

  private OdkSyncServiceProxy proxy = null;

  public boolean shouldWaitForDebugger() {
    return debugService;
  }
  
  public synchronized OdkSyncServiceProxy getOdkSyncServiceProxy() {
    if (proxy == null) {
      proxy = new OdkSyncServiceProxy(this);
    }
    return proxy;
  }

  public synchronized void resetOdkSyncServiceProxy() {
    if (proxy != null) {
      proxy.shutdown();
      proxy = null;
    }
  }

  @Override
  public void onCreate() {
    singleton = this;
    super.onCreate();
  }

  @Override
  public int getApkDisplayNameResourceId() {
    return R.string.app_name;
  }

  @Override
  public int getConfigZipResourceId() {
    // Ignored -- needs handling in InitializationTask
    return -1;
  }

  @Override
  public int getSystemZipResourceId() {
    // Ignored -- needs handling in InitializationTask
    return -1;
  }

  @Override
  public int getWebKitResourceId() {
    return -1;
  }

}
