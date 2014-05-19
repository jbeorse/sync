/*
 * Copyright (C) 2012 University of Washington
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opendatakit.sync.OdkSyncServiceProxy;
import org.opendatakit.sync.R;
import org.opendatakit.sync.SyncConsts;
import org.opendatakit.sync.SyncPreferences;
import org.opendatakit.sync.SynchronizationResult;
import org.opendatakit.sync.SynchronizationResult.Status;
import org.opendatakit.sync.TableResult;
import org.opendatakit.sync.files.SyncUtil;
import org.opendatakit.sync.service.SyncProgressState;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.DialogInterface.OnClickListener;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.RemoteException;
import android.util.Log;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.Toast;

/**
 * An activity for downloading from and uploading to an ODK Aggregate instance.
 * 
 * @author hkworden@gmail.com
 * @author the.dylan.price@gmail.com
 */
public class SyncActivity extends Activity {

  private static final String LOGTAG = SyncActivity.class.getSimpleName();

  private static final String ACCOUNT_TYPE_G = "com.google";
  private static final String URI_FIELD_EMPTY = "http://";

  private static final int AUTHORIZE_ACCOUNT_RESULT_ID = 1;

  private EditText uriField;
  private Spinner accountListSpinner;

  private String appName;
  private AccountManager accountManager;

  private OdkSyncServiceProxy syncProxy;

  private TextView progressState;
  private TextView progressMessage;

  private UpdateStatusTask updateStatusTask;

  @Override
  public void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    appName = getIntent().getStringExtra(SyncConsts.INTENT_KEY_APP_NAME);
    if (appName == null) {
      appName = SyncUtil.getDefaultAppName();
    }
    accountManager = AccountManager.get(this);

    syncProxy = new OdkSyncServiceProxy(this);

    setTitle("");
    setContentView(R.layout.aggregate_activity);
    findViewComponents();
    try {
      SyncPreferences prefs = new SyncPreferences(this, appName);
      initializeData(prefs);
      updateButtonsEnabled(prefs);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    updateStatusTask = new UpdateStatusTask();
    updateStatusTask.execute();

  }

  @Override
  protected void onStart() {
    super.onStart();
    try {
      SyncPreferences prefs = new SyncPreferences(this, appName);
      updateButtonsEnabled(prefs);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  protected void onResume() {
    super.onResume();
    try {
      SyncPreferences prefs = new SyncPreferences(this, appName);
      updateButtonsEnabled(prefs);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void findViewComponents() {
    uriField = (EditText) findViewById(R.id.aggregate_activity_uri_field);
    accountListSpinner = (Spinner) findViewById(R.id.aggregate_activity_account_list_spinner);
    progressState = (TextView) findViewById(R.id.aggregate_activity_progress_state);
    progressMessage = (TextView) findViewById(R.id.aggregate_activity_progress_message);
  }

  private void initializeData(SyncPreferences prefs) {
    // Add accounts to spinner
    Account[] accounts = accountManager.getAccountsByType(ACCOUNT_TYPE_G);
    List<String> accountNames = new ArrayList<String>(accounts.length);
    for (int i = 0; i < accounts.length; i++)
      accountNames.add(accounts[i].name);

    ArrayAdapter<String> adapter = new ArrayAdapter<String>(this,
        android.R.layout.select_dialog_item, accountNames);
    accountListSpinner.setAdapter(adapter);

    // Set saved server url
    String serverUri = prefs.getServerUri();

    if (serverUri == null)
      uriField.setText(URI_FIELD_EMPTY);
    else
      uriField.setText(serverUri);

    // Set chosen account
    String accountName = prefs.getAccount();
    if (accountName != null) {
      int index = accountNames.indexOf(accountName);
      accountListSpinner.setSelection(index);
    }
  }

  void updateButtonsEnabled(SyncPreferences prefs) {
    String accountName = prefs.getAccount();
    String serverUri = prefs.getServerUri();
    boolean haveSettings = (accountName != null) && (serverUri != null);
    boolean authorizeAccount = (accountName != null) && (prefs.getAuthToken() == null);

    boolean restOfButtons = haveSettings && !authorizeAccount;

    findViewById(R.id.aggregate_activity_save_settings_button).setEnabled(true);
    findViewById(R.id.aggregate_activity_authorize_account_button).setEnabled(authorizeAccount);
    findViewById(R.id.aggregate_activity_sync_now_push_button).setEnabled(restOfButtons);
    findViewById(R.id.aggregate_activity_sync_now_pull_button).setEnabled(restOfButtons);
  }

  private void saveSettings(SyncPreferences prefs) throws IOException {

    // save fields in preferences
    String uri = uriField.getText().toString();
    if (uri.equals(URI_FIELD_EMPTY))
      uri = null;
    String accountName = (String) accountListSpinner.getSelectedItem();

    prefs.setServerUri(uri);
    prefs.setAccount(accountName);

  }

  AlertDialog.Builder buildOkMessage(String title, String message) {
    AlertDialog.Builder builder = new AlertDialog.Builder(this);
    builder.setCancelable(false);
    builder.setPositiveButton(getString(R.string.ok), null);
    builder.setTitle(title);
    builder.setMessage(message);
    return builder;
  }

  /**
   * Get a string to display to the user based on the {@link TableResult} after
   * a sync. Handles the logic for generating an appropriate message.
   * <p>
   * Presents something along the lines of: Your Table: Insert on the
   * server--Success. Your Table: Pulled data from server. Failed to push
   * properties. Etc.
   * 
   * @param context
   * @param result
   * @return
   */
  private static String getMessageForTableResult(Context context, TableResult result) {
    StringBuilder msg = new StringBuilder();
    msg.append(result.getTableDisplayName() + ": ");
    msg.append(context.getString(R.string.sync_action_message_insert) + "--");
    // Now add the result of the status.

    Status status = result.getStatus();
    String name;
    switch (status) {
    case EXCEPTION:
      name = context.getString(R.string.sync_table_result_exception);
      break;
    case FAILURE:
      name = context.getString(R.string.sync_table_result_failure);
      break;
    case SUCCESS:
      name = context.getString(R.string.sync_table_result_success);
      break;
    default:
      Log.e(LOGTAG, "unrecognized TableResult status: " + status + ". Setting " + "to failure.");
      name = context.getString(R.string.sync_table_result_failure);
    }

    msg.append(name);
    if (result.getStatus() == TableResult.Status.EXCEPTION) {
      // We'll append the message as well.
      msg.append(result.getMessage());
    }
    msg.append("--");
    // Now we need to add some information about the individual actions that
    // should have been performed.
    if (result.hadLocalDataChanges()) {
      if (result.pushedLocalData()) {
        msg.append("Pushed local data. ");
      } else {
        msg.append("Failed to push local data. ");
      }
    } else {
      msg.append("No local data changes. ");
    }

    if (result.hadLocalPropertiesChanges()) {
      if (result.pushedLocalProperties()) {
        msg.append("Pushed local properties. ");
      } else {
        msg.append("Failed to push local properties. ");
      }
    } else {
      msg.append("No local properties changes. ");
    }

    if (result.serverHadDataChanges()) {
      if (result.pulledServerData()) {
        msg.append("Pulled data from server. ");
      } else {
        msg.append("Failed to pull data from server. ");
      }
    } else {
      msg.append("Data not re-fetched from server. ");
    }

    if (result.serverHadPropertiesChanges()) {
      if (result.pulledServerProperties()) {
        msg.append("Pulled properties from server. ");
      } else {
        msg.append("Failed to pull properties from server. ");
      }
    } else {
      msg.append("Properties not re-fetched from server.");
    }

    if (result.serverHadSchemaChanges()) {
      if (result.pulledServerSchema()) {
        msg.append("Pulled schema from server. ");
      } else {
        msg.append("Failed to pull schema from server. ");
      }
    } else {
      msg.append("Schema not re-fetched from server.");
    }

    return msg.toString();
  }

  AlertDialog.Builder buildResultMessage(String title, SynchronizationResult result) {
    AlertDialog.Builder builder = new AlertDialog.Builder(this);
    builder.setCancelable(false);
    builder.setPositiveButton(getString(R.string.ok), null);
    builder.setTitle(title);
    // Now we'll make the message. This should include the contents of the
    // result parameter.
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < result.getTableResults().size(); i++) {
      TableResult tableResult = result.getTableResults().get(i);
      stringBuilder.append(getMessageForTableResult(this, tableResult));
      // stringBuilder.append(tableResult.getTableDisplayName() + ": " +
      // SyncUtil.getLocalizedNameForTableResultStatus(this,
      // tableResult.getStatus()));
      // if (tableResult.getStatus() == Status.EXCEPTION) {
      // stringBuilder.append(" with message: " +
      // tableResult.getMessage());
      // }
      if (i < result.getTableResults().size() - 1) {
        // only append if we have a
        stringBuilder.append("\n");
      }
    }
    builder.setMessage(stringBuilder.toString());
    return builder;
  }

  /**
   * Hooked up to save settings button in aggregate_activity.xml
   */
  public void onClickSaveSettings(View v) {
    try {
      final SyncPreferences prefs = new SyncPreferences(this, appName);
      // show warning message
      AlertDialog.Builder msg = buildOkMessage(getString(R.string.confirm_change_settings),
          getString(R.string.change_settings_warning));

      msg.setPositiveButton(getString(R.string.save), new OnClickListener() {
        @Override
        public void onClick(DialogInterface dialog, int which) {

          // TODO: IMPORATNT rewrite this interaction
          try {
            saveSettings(prefs);

            // SS Oct 15: clear the auth token here.
            // TODO if you change a user you can switch to their privileges
            // without
            // this.
            Log.d(LOGTAG, "[onClickSaveSettings][onClick] invalidated authtoken");
            invalidateAuthToken(prefs.getAuthToken(), SyncActivity.this, appName);
            updateButtonsEnabled(prefs);
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

        }
      });

      msg.setNegativeButton(getString(R.string.cancel), null);
      msg.show();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Hooked up to authorizeAccountButton's onClick in aggregate_activity.xml
   */
  public void onClickAuthorizeAccount(View v) {
    try {
      SyncPreferences prefs = new SyncPreferences(this, appName);
      Intent i = new Intent(this, AccountInfoActivity.class);
      Account account = new Account(prefs.getAccount(), ACCOUNT_TYPE_G);
      i.putExtra(SyncConsts.INTENT_KEY_APP_NAME, appName);
      i.putExtra(AccountInfoActivity.INTENT_EXTRAS_ACCOUNT, account);
      startActivityForResult(i, AUTHORIZE_ACCOUNT_RESULT_ID);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Hooked to syncNowButton's onClick in aggregate_activity.xml
   */
  public void onClickSyncNowPush(View v) {
    Log.d(LOGTAG, "in onClickSyncNowPush");
    // ask whether to sync app files and table-level files

    try {
      SyncPreferences prefs = new SyncPreferences(this, appName);
      String accountName = prefs.getAccount();
      Log.e(LOGTAG, "[onClickSyncNowPush] timestamp: " + System.currentTimeMillis());
      if (accountName == null) {
        Toast.makeText(this, getString(R.string.choose_account), Toast.LENGTH_SHORT).show();
      } else {
        try {
          syncProxy.pushToServer(appName);
        } catch (RemoteException e) {
          Log.e(LOGTAG, "Problem with push command");
        }

      }
      updateButtonsEnabled(prefs);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Hooked to syncNowButton's onClick in aggregate_activity.xml
   */
  public void onClickSyncNowPull(View v) {
    Log.d(LOGTAG, "in onClickSyncNowPull");
    // ask whether to sync app files and table-level files
    try {
      SyncPreferences prefs = new SyncPreferences(this, appName);
      String accountName = prefs.getAccount();
      Log.e(LOGTAG, "[onClickSyncNowPull] timestamp: " + System.currentTimeMillis());
      if (accountName == null) {
        Toast.makeText(this, getString(R.string.choose_account), Toast.LENGTH_SHORT).show();
      } else {
        try {
          syncProxy.synchronizeFromServer(appName);
        } catch (RemoteException e) {
          Log.e(LOGTAG, "Problem with pull command");
        }
      }
      updateButtonsEnabled(prefs);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public static void invalidateAuthToken(String authToken, Context context, String appName) {
    AccountManager.get(context).invalidateAuthToken(ACCOUNT_TYPE_G, authToken);
    try {
      SyncPreferences prefs = new SyncPreferences(context, appName);
      prefs.setAuthToken(null);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  protected void onActivityResult(int requestCode, int resultCode, Intent data) {
    try {
      SyncPreferences prefs = new SyncPreferences(this, appName);
      super.onActivityResult(requestCode, resultCode, data);
      updateButtonsEnabled(prefs);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void updateProgress() {
    runOnUiThread(new Runnable() {
      public void run() {

        try {

          SyncProgressState progress = syncProxy.getSyncProgress(appName);
          if (progressState != null) {
            if (progress == null) {
              progressState.setText("NULL");
            } else {
              progressState.setText(progress.name());
            }
          } else {
            Log.e(LOGTAG, "NULL progressState variable");
          }

          String msg = syncProxy.getSyncUpdateMessage(appName);
          if (progressMessage != null) {
            if (progress == null) {
              progressMessage.setText("NULL");
            } else {
              progressMessage.setText(msg);
            }
          } else {
            Log.e(LOGTAG, "NULL progressMessage variable");
          }
        } catch (RemoteException e) {
          Log.e(LOGTAG, "Problem with update messages");
        }
      }
    });

  }

  private class UpdateStatusTask extends AsyncTask<Void, Void, Void> {

    private static final int DELAY_PROGRESS_REFRESH = 20000;

    @Override
    protected Void doInBackground(Void... params) {

      try {
        while (true) {
          Thread.sleep(DELAY_PROGRESS_REFRESH);
          //Log.e(LOGTAG, "Wake up");
          updateProgress();
        }
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return null;

    }
  }

}
