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
package org.opendatakit.conflict.activities;

import java.util.Set;
import java.util.TreeSet;

import org.opendatakit.common.android.activities.BaseListActivity;
import org.opendatakit.common.android.application.CommonApplication;
import org.opendatakit.common.android.data.OrderedColumns;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.UserTable.Row;
import org.opendatakit.common.android.listener.DatabaseConnectionListener;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.database.service.OdkDbHandle;
import org.opendatakit.sync.application.Sync;

import android.content.Intent;
import android.os.RemoteException;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.ListView;

/**
 * An activity for presenting a list of all the rows in conflict.
 *
 * @author sudar.sam@gmail.com
 *
 */
public class CheckpointResolutionListActivity extends BaseListActivity implements DatabaseConnectionListener {

  private static final String TAG = CheckpointResolutionListActivity.class.getSimpleName();

  private static final int RESOLVE_ROW = 1;

  private String mAppName;
  private String mTableId;
  private ArrayAdapter<ResolveRowEntry> mAdapter;

  private static final class ResolveRowEntry {
    final String rowId;
    final String displayName;

    ResolveRowEntry(String rowId, String displayName) {
      this.rowId = rowId;
      this.displayName = displayName;
    }

    public String toString() {
      return displayName;
    }
  };

  @Override
  protected void onResume() {
    super.onResume();
    // Do this in on resume so that if we resolve a row it will be refreshed
    // when we come back.
    mAppName = getIntent().getStringExtra(Constants.APP_NAME);
    if (mAppName == null) {
      mAppName = Constants.DEFAULT_APP_NAME;
    }
    mTableId = getIntent().getStringExtra(Constants.TABLE_ID);
  }
  
  @Override
  protected void onPostResume() {
    super.onPostResume();
    ((CommonApplication) getApplication()).establishDatabaseConnectionListener(this);
  }

  @Override
  protected void onActivityResult(int requestCode, int resultCode, Intent data) {
    super.onActivityResult(requestCode, resultCode, data);
  }

  @Override
  protected void onListItemClick(ListView l, View v, int position, long id) {
    ResolveRowEntry e = mAdapter.getItem(position);
    WebLogger.getLogger(mAppName).e(TAG,
        "[onListItemClick] clicked position: " + position + " rowId: " + e.rowId);
    launchRowResolution(e);
  }

  private void launchRowResolution(ResolveRowEntry e) {
    Intent i = new Intent(this, CheckpointResolutionRowActivity.class);
    i.putExtra(Constants.APP_NAME, mAppName);
    i.putExtra(Constants.TABLE_ID, mTableId);
    i.putExtra(CheckpointResolutionRowActivity.INTENT_KEY_ROW_ID, e.rowId);
    this.startActivityForResult(i, RESOLVE_ROW);
  }

  @Override
  public String getAppName() {
    return mAppName;
  }

  @Override
  public void databaseAvailable() {

    if ( Sync.getInstance().getDatabase() == null ) {
      return;
    }
    
    OdkDbHandle db = null;
    UserTable table = null;
    try {
      db = Sync.getInstance().getDatabase().openDatabase(mAppName, false);
      OrderedColumns orderedDefns = Sync.getInstance().getDatabase().getUserDefinedColumns(
          mAppName, db, mTableId);
      String[] empty = {};
      table = Sync.getInstance().getDatabase().rawSqlQuery(mAppName, db, mTableId, orderedDefns,
          DataTableColumns.SAVEPOINT_TYPE + " IS NULL", empty, empty, null, DataTableColumns.ID,
          "ASC");
    } catch (RemoteException e) {
      WebLogger.getLogger(mAppName).printStackTrace(e);
      WebLogger.getLogger(mAppName).e(TAG, "database access error");
      setResult(RESULT_CANCELED);
      finish();
      return;
    } finally {
      if ( db != null ) {
        try {
          Sync.getInstance().getDatabase().closeDatabase(mAppName, db);
        } catch (RemoteException e) {
          WebLogger.getLogger(mAppName).printStackTrace(e);
          WebLogger.getLogger(mAppName).e(TAG, "database access error");
          setResult(RESULT_CANCELED);
          finish();
          return;
        }
      }
    }
    if (table != null) {
      this.mAdapter = new ArrayAdapter<ResolveRowEntry>(getActionBar().getThemedContext(),
          android.R.layout.simple_list_item_1);
      Set<String> rowIds = new TreeSet<String>();
      for (int i = 0; i < table.getNumberOfRows(); i++) {
        Row row = table.getRowAtIndex(i);
        String rowId = row.getRawDataOrMetadataByElementKey(DataTableColumns.ID);
        rowIds.add(rowId);
      }
      if (rowIds.isEmpty()) {
        this.setResult(RESULT_OK);
        finish();
        return;
      }

      ResolveRowEntry firstE = null;
      int i = 0;
      for (String rowId : rowIds) {
        ++i;
        ResolveRowEntry e = new ResolveRowEntry(rowId, "Resolve ODK Survey Checkpoint Record " + i);
        this.mAdapter.add(e);
        if (firstE == null) {
          firstE = e;
        }
      }
      this.setListAdapter(mAdapter);

      if (rowIds.size() == 1) {
        launchRowResolution(firstE);
      }
    }
  }

  @Override
  public void databaseUnavailable() {
    // TODO Auto-generated method stub
    
  }

}
