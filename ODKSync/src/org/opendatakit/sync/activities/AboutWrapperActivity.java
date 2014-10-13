/*
 * Copyright (C) 2014 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.opendatakit.sync.activities;

import org.opendatakit.sync.R;
import org.opendatakit.sync.SyncConsts;
import org.opendatakit.sync.files.SyncUtil;

import android.app.Activity;
import android.os.Bundle;

public class AboutWrapperActivity extends Activity {

  private String appName;

  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    appName = getIntent().getStringExtra(SyncConsts.INTENT_KEY_APP_NAME);
    if (appName == null) {
      appName = SyncUtil.getDefaultAppName();
    }

    // see if we saved the state
    this.setContentView(R.layout.about_wrapper_activity);
  }

  public String getAppName() {
    return appName;
  }

}
