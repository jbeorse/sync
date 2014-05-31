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
package org.opendatakit.sync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.Scope;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinitionResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.common.android.data.ColumnProperties;
import org.opendatakit.common.android.data.ColumnType;
import org.opendatakit.common.android.data.DataHelper;
import org.opendatakit.common.android.data.DbTable;
import org.opendatakit.common.android.data.TableDefinitions;
import org.opendatakit.common.android.data.TableProperties;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.UserTable.Row;
import org.opendatakit.common.android.database.DataModelDatabaseHelper;
import org.opendatakit.common.android.database.DataModelDatabaseHelperFactory;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.sync.aggregate.SyncTag;
import org.opendatakit.common.android.sync.exceptions.SchemaMismatchException;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utils.CsvUtil;
import org.opendatakit.common.android.utils.DataUtil;
import org.opendatakit.sync.SynchronizationResult.Status;
import org.opendatakit.sync.Synchronizer.OnTablePropertiesChanged;
import org.opendatakit.sync.Synchronizer.SynchronizerStatus;
import org.opendatakit.sync.service.SyncNotification;
import org.opendatakit.sync.service.SyncProgressState;
import org.springframework.web.client.ResourceAccessException;

import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.util.Log;

/**
 * SyncProcessor implements the cloud synchronization logic for Tables.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class SyncProcessor implements SynchronizerStatus {

  private static final String TAG = SyncProcessor.class.getSimpleName();

  private static final int ROWS_BETWEEN_PROGRESS_UPDATES = 10;
  private static final int OVERALL_PROGRESS_BAR_LENGTH = 6350400;
  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
  }

  private int nMajorSyncSteps;
  private int iMajorSyncStep;
  private int GRAINS_PER_MAJOR_SYNC_STEP;

  private final Context context;
  private final String appName;
  private final DataUtil du;
  private final SyncNotification syncProgress;
  private final Synchronizer synchronizer;
  /**
   * The results of the synchronization that we will pass back to the user.
   */
  private final SynchronizationResult mUserResult;

  public SyncProcessor(Context context, String appName, Synchronizer synchronizer, SyncNotification syncProgress) {
    this.context = context;
    this.appName = appName;
    this.du = new DataUtil(Locale.ENGLISH, TimeZone.getDefault());
    this.syncProgress = syncProgress;
    this.synchronizer = synchronizer;
    this.mUserResult = new SynchronizationResult();
  }

  public SynchronizationResult getOverallResults() {
    return mUserResult;
  }

  @Override
  public void updateNotification(SyncProgressState state, int textResource, Object[] formatArgVals, Double progressPercentage, boolean indeterminateProgress) {
    String text = "Bad text resource id: " + textResource + "!";
    String fmt = this.context.getString(textResource);
    if ( fmt != null ) {
      if ( formatArgVals == null ) {
        text = fmt;
      } else {
        text = String.format(fmt, formatArgVals);
      }
    }
    syncProgress.updateNotification(state, text,
        OVERALL_PROGRESS_BAR_LENGTH, (int) (iMajorSyncStep * GRAINS_PER_MAJOR_SYNC_STEP +
                  ((progressPercentage != null) ?
                     (progressPercentage * GRAINS_PER_MAJOR_SYNC_STEP / 100.0) : 0.0)), indeterminateProgress);
  }

  /**
   * Synchronize all app-level files and all data table schemas and table-level
   * files.
   *
   * This synchronization sets the stage for data row synchronization. The two
   * modes are either to pull all this configuration down from the server and
   * enforce that the client contain all files and tables on the server or to
   * enforce that the server contains all files and tables that are on the
   * client.
   *
   * When pulling down (the normal mode of operation), we reload the local
   * properties from the tables/tableId/properties.csv that has been pulled down
   * from the server.
   *
   * This does not process zip files; it is unclear whether we should do
   * anything for those or just leave them as zip files locally.
   */
  public void synchronizeConfigurationAndContent(boolean pushToServer) {
    Log.i(TAG, "entered synchronizeConfigurationAndContent()");
    ODKFileUtils.assertDirectoryStructure(appName);

    syncProgress.updateNotification(SyncProgressState.INIT,
        context.getString(R.string.retrieving_tables_list_from_server), OVERALL_PROGRESS_BAR_LENGTH, 0, false);

    // get tables (tableId -> schemaETag) from server
    List<TableResource> tables = new ArrayList<TableResource>();
    try {
      tables = synchronizer.getTables();
      if (tables == null) {
        tables = new ArrayList<TableResource>();
      }
    } catch (ResourceAccessException e) {
      mUserResult.setAppLevelStatus(Status.AUTH_EXCEPTION);
      Log.i(TAG, "[synchronizeConfigurationAndContent] Could not retrieve server table list", e);
      return;
    } catch (Exception e) {
      mUserResult.setAppLevelStatus(Status.EXCEPTION);
      Log.e(TAG, "[synchronizeConfigurationAndContent] Unexpected exception getting server table list", e);
      return;
    }

    // TODO: do the database updates with a few database transactions...

    // get the tables on the local device
    List<String> localTableIds = new ArrayList<String>();
    try {
      DataModelDatabaseHelper dbHelper = DataModelDatabaseHelperFactory.getDbHelper(context, appName);
      SQLiteDatabase db = dbHelper.getReadableDatabase();
      localTableIds = TableDefinitions.getAllTableIds(db);
      db.close();
    } catch (SQLiteException e ) {
      mUserResult.setAppLevelStatus(Status.EXCEPTION);
      Log.e(TAG, "[synchronizeConfigurationAndContent] Unexpected exception getting local tableId list", e);
      return;
    }

    // Figure out how many major steps there are to the sync
    {
      Set<String> uniqueTableIds = new HashSet<String>();
      uniqueTableIds.addAll(localTableIds);
      for ( TableResource table : tables ) {
        uniqueTableIds.add(table.getTableId());
      }
      // when pushing, we never drop tables on the server (but never pull those either).
      // i.e., pushing only adds to the set of tables on the server.
      //
      // when pulling, we drop all local tables that do not match the server, and pull
      // everything from the server.
      nMajorSyncSteps = 1 +
          (pushToServer ? 2*localTableIds.size() : (uniqueTableIds.size() + tables.size()));
      GRAINS_PER_MAJOR_SYNC_STEP = (OVERALL_PROGRESS_BAR_LENGTH / nMajorSyncSteps);
    }
    iMajorSyncStep = 0;

    // First we're going to synchronize the app level files.
    try {
      boolean success = synchronizer.syncAppLevelFiles(pushToServer, this);
      mUserResult.setAppLevelStatus(success ? Status.SUCCESS : Status.FAILURE);
    } catch (ResourceAccessException e) {
      // TODO: update a synchronization result to report back to them as well.
      mUserResult.setAppLevelStatus(Status.AUTH_EXCEPTION);
      Log.e(TAG,
          "[synchronizeConfigurationAndContent] error trying to synchronize app-level files.");
      e.printStackTrace();
      return;
    }

    // done with app-level file synchronization
    ++iMajorSyncStep;

    if (pushToServer) {
      // ///////////////////////////////////////////
      // / UPDATE SERVER CONTENT
      // / UPDATE SERVER CONTENT
      // / UPDATE SERVER CONTENT
      // / UPDATE SERVER CONTENT
      // / UPDATE SERVER CONTENT
      for (String localTableId : localTableIds) {
        TableResource matchingResource = null;
        for (TableResource tr : tables) {
          if (tr.getTableId().equals(localTableId)) {
            matchingResource = tr;
            break;
          }
        }
        Log.i(TAG, "[synchronizeConfigurationAndContent] synchronizing table " + localTableId);

        TableProperties tp = TableProperties.refreshTablePropertiesForTable(context, appName, localTableId);
        if ( tp.getDbTableName() == null ) {
          tp = null;
        }
        synchronizeTableConfigurationAndContent(tp, matchingResource, true);
        this.updateNotification(SyncProgressState.TABLE_FILES, R.string.table_level_file_sync_complete, new Object[] {localTableId}, 100.0, false);
        ++iMajorSyncStep;
      }
    } else {
      // //////////////////////////////////////////
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT

      List<String> localTableIdsToDelete = new ArrayList<String>();
      localTableIdsToDelete.addAll(localTableIds);

      --iMajorSyncStep;
      for (TableResource table : tables) {
        ++iMajorSyncStep;
        TableProperties tp = null;

        String serverTableId = table.getTableId();
        for ( String localTableId : localTableIds ) {
          if (localTableId.equals(serverTableId)) {
            localTableIdsToDelete.remove(localTableId);
            tp = TableProperties.refreshTablePropertiesForTable(context, appName, localTableId);
            if ( tp.getDbTableName() == null ) {
              tp = null;
            }
            break;
          }
        }
        TableResult tableResult = mUserResult.getTableResult(serverTableId);

        updateNotification(SyncProgressState.TABLE_FILES, ((tp == null) ?
            R.string.creating_local_table : R.string.verifying_table_schema_on_server),
            new Object[] { serverTableId },
            0.0, false);

        try {
          TableDefinitionResource definitionResource = synchronizer.getTableDefinition(table
              .getDefinitionUri());

          tp = addTableFromDefinitionResource(definitionResource, tp);
        } catch (JsonParseException e) {
          e.printStackTrace();
          tableResult.setStatus(Status.EXCEPTION);
          Log.e(TAG, "[synchronizeConfigurationAndContent] Unexpected exception parsing table definition", e);
          continue;
        } catch (JsonMappingException e) {
          e.printStackTrace();
          tableResult.setStatus(Status.EXCEPTION);
          Log.e(TAG, "[synchronizeConfigurationAndContent] Unexpected exception parsing table definition", e);
          continue;
        } catch (IOException e) {
          e.printStackTrace();
          tableResult.setStatus(Status.EXCEPTION);
          Log.e(TAG, "[synchronizeConfigurationAndContent] Unexpected exception accessing table definition", e);
          continue;
        } catch (SchemaMismatchException e) {
          e.printStackTrace();
          tableResult.setStatus(Status.EXCEPTION);
          Log.e(TAG, "[synchronizeConfigurationAndContent] The schema for this table does not match that on the server", e);
          continue;
        }

        // Sync the local media files with the server if the table
        // existed locally before we attempted downloading it.

        synchronizeTableConfigurationAndContent(tp, table, false);
        this.updateNotification(SyncProgressState.TABLE_FILES, R.string.table_level_file_sync_complete, new Object[] {serverTableId}, 100.0, false);
      }
      ++iMajorSyncStep;

      // and now loop through the ones to delete...
      for (String localTableId : localTableIdsToDelete) {
        updateNotification(SyncProgressState.TABLE_FILES, R.string.dropping_local_table, new Object[] {localTableId},
            0.0, false);
        TableProperties tp = TableProperties.refreshTablePropertiesForTable(context, appName, localTableId);
        // eventually might not be true if there are multiple syncs running simultaneously...
        if ( tp.getDbTableName() != null ) {
          TableResult tableResult = mUserResult.getTableResult(localTableId);
          tp.deleteTable();
          tableResult.setStatus(Status.SUCCESS);
        }
        ++iMajorSyncStep;
      }
    }
  }

  /**
   * Synchronize the table represented by the given TableProperties with the
   * cloud.
   * <p>
   * Note that if the db changes under you when calling this method, the tp
   * parameter will become out of date. It should be refreshed after calling
   * this method.
   * <p>
   * This method does NOT synchronize the framework files. The management of
   * the contents of the framework directory is managed by the individual
   * APKs themselves.
   *
   * @param tp
   *          the table to synchronize
   * @param pushLocalTableLevelFiles
   *          true if local table-level files should be pushed up to the server.
   *          e.g. any html files on the device should be pushed to the server
   * @param pushLocalInstanceFiles
   *          if local media files associated with data rows should be pushed up
   *          to the server. The data files on the server are always pulled
   *          down.
   */
  private void synchronizeTableConfigurationAndContent(TableProperties tp, TableResource resource,
      boolean pushLocalTableLevelFiles) {

    // used to get the above from the ACTIVE store. if things go wonky, maybe
    // check to see if it was ACTIVE rather than SERVER for a reason. can't
    // think of one. one thing is that if it fails you'll see a table but won't
    // be able to open it, as there won't be any KVS stuff appropriate for it.
    boolean success = false;
    // Prepare the tableResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    String tableId = tp.getTableId();

    this.updateNotification(SyncProgressState.TABLE_FILES, R.string.verifying_table_schema_on_server, new Object[] {tableId}, 0.0, false);
    TableResult tableResult = mUserResult.getTableResult(tableId);
    if ( tp != null ) {
      tableResult.setTableDisplayName(tp.getLocalizedDisplayName());
    }
    beginTableTransaction(tp);
    try {
      // Confirm that the local schema matches that on the server...
      // If we are pushing to the server, create it on the server.
      if (resource == null) {

        if (!pushLocalTableLevelFiles) {
          // the table on the server is missing. Need to ask user what to do...
          tableResult.setServerHadSchemaChanges(true);
          tableResult
              .setMessage("Server no longer has table! Deleting it locally. Reset App Server to upload.");
          tableResult.setStatus(Status.TABLE_DOES_NOT_EXIST_ON_SERVER);
          return;
        }

        // the insert of the table was incomplete -- try again

        // we are creating data on the server
        // change row sync and conflict status to handle new server schema.
        // Clean up this table and set the dataETag to null.
        DbTable dbt = DbTable.getDbTable(tp);
        dbt.changeDataRowsToInsertingState();

        // we need to clear out the dataETag so
        // that we will pull all server changes and sync our properties.
        SyncTag newSyncTag = new SyncTag(null, tp.getSyncTag().getSchemaETag());
        tp.setSyncTag(newSyncTag);
        /**************************
         * PART 1A: CREATE THE TABLE First we need to create the table on the
         * server. This comes in two parts--the definition and the properties.
         **************************/
        // First create the table definition on the server.
        try {
          resource = synchronizer.createTable(tp.getTableId(), newSyncTag, getColumnsForTable(tp));
        } catch (Exception e) {
          e.printStackTrace();
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        }

        SyncTag syncTag = new SyncTag(null, resource.getSchemaETag());
        tp.setSyncTag(syncTag);
      }

      // we found the matching resource on the server and we have set up our
      // local table to be ready for any data merge with the server's table.

      SyncTag syncTag = tp.getSyncTag();

      /**************************
       * PART 1A: UPDATE THE TABLE SCHEMA. We only do this if necessary. Do this
       * before updating data in case columns have changed or something specific
       * applies. These updates come in two parts: the table definition, and the
       * table properties (i.e. the key value store).
       **************************/
      if (syncTag == null || !resource.getSchemaETag().equals(syncTag.getSchemaETag())) {
        Log.d(TAG, "updateDbFromServer setServerHadSchemaChanges(true)");
        tableResult.setServerHadSchemaChanges(true);

        // fetch the table definition
        TableDefinitionResource definitionResource;
        try {
          definitionResource = synchronizer.getTableDefinition(resource.getDefinitionUri());
        } catch (Exception e) {
          e.printStackTrace();
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        }

        // record that we have pulled it
        tableResult.setPulledServerSchema(true);
        try {
          // apply changes
          // this also updates the data rows so they will sync
          tp = addTableFromDefinitionResource(definitionResource, tp);

          Log.w(TAG,
              "database schema has changed. Structural modifications, if any, were successful.");
        } catch (SchemaMismatchException e) {
          e.printStackTrace();
          Log.w(TAG, "database properties have changed. "
              + "structural modifications were not successful. You must delete the table"
              + " and download it to receive the updates.");
          tableResult.setMessage(e.toString());
          tableResult.setStatus(Status.FAILURE);
          return;
        } catch (JsonParseException e) {
          e.printStackTrace();
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        } catch (JsonMappingException e) {
          e.printStackTrace();
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        } catch (IOException e) {
          e.printStackTrace();
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        }
      }

      // OK. we have the schemaETag matching.

      // write our properties and definitions files.
      final CsvUtil utils = new CsvUtil(context, appName);
      // write the current schema and properties set.
      utils.writePropertiesCsv(tp);

      synchronizer.syncTableLevelFiles(tp.getTableId(), new OnTablePropertiesChanged() {
        @Override
        public void onTablePropertiesChanged(String tableId) {
          utils.updateTablePropertiesFromCsv(null, tableId);
        }
      }, pushLocalTableLevelFiles, this);

      // we found the matching resource on the server and we have set up our
      // local table to be ready for any data merge with the server's table.

      // we should be up-to-date on the schema and properties
      // now fetch all the changed rows...

      // refresh the tp
      tp = TableProperties.refreshTablePropertiesForTable(context, appName, tp.getTableId());
      if ( tp.getDbTableName() == null ) {
        tp = null;
      }

      success = true;
    } finally {
      endTableTransaction(tp, success);
      if (success && tableResult.getStatus() != Status.WORKING) {
          Log.e(TAG, "tableResult status for table: " + tp.getDbTableName() +
              " was " + tableResult.getStatus().name() +
              ", and yet success returned true. This shouldn't be possible.");
      }
    }
  }

  /**
   * Synchronize all synchronized tables with the cloud.
   * <p>
   * This becomes more complicated with the ability to synchronize files. The
   * new order is as follows:
   * <ol>
   * <li>Synchronize app-level files. (i.e. those files under the appid
   * directory that are NOT then under the tables, instances, metadata, or
   * logging directories.) This is a multi-part process:
   * <ol>
   * <li>Get the app-level manifest, download any files that have changed
   * (differing hashes) or that do not exist.</li>
   * <li>Upload the files that you have that are not on the manifest. Note that
   * this could be suppressed if the user does not have appropriate permissions.
   * </li>
   * </ol>
   * </li>
   *
   * <li>Synchronize the static table files for those tables that are set to
   * sync. (i.e. those files under "appid/tables/tableid"). This follows the
   * same multi-part steps above (1a and 1b).</li>
   *
   * <li>Synchronize the table properties/metadata.</li>
   *
   * <li>Synchronize the table data. This includes the data in the db as well as
   * those files under "appid/instances/tableid". This file synchronization
   * follows the same multi-part steps above (1a and 1b).</li>
   *
   * <li>TODO: step four--the synchronization of instances files--should perhaps
   * also be allowed to be modular and permit things like ODK Submit to handle
   * data and files separately.</li>
   * </ol>
   * <p>
   * TODO: This should also somehow account for zipped files, exploding them or
   * what have you.
   * </p>
   */
  public void synchronizeDataRowsAndAttachments() {
    Log.i(TAG, "entered synchronize()");
    ODKFileUtils.assertDirectoryStructure(appName);

    if ( mUserResult.getAppLevelStatus() != Status.SUCCESS ) {
      Log.e(TAG, "Abandoning data row update -- app-level sync was not successful!");
      return;
    }

    TableProperties[] tps;
    tps = TableProperties.getTablePropertiesForAll(context, appName);

    // we can assume that all the local table properties should
    // sync with the server.
    for (TableProperties tp : tps) {
      // Sync the local media files with the server if the table
      // existed locally before we attempted downloading it.

      synchronizeTableDataRowsAndAttachments(tp);
      ++iMajorSyncStep;
    }
  }

  private Double perRowIncrement;
  private int rowsProcessed;

  /**
   * Synchronize the table data rows.
   * <p>
   * Note that if the db changes under you when calling this method, the tp
   * parameter will become out of date. It should be refreshed after calling
   * this method.
   * <p>
   * This method does NOT synchronize any non-instance files; it assumes
   * the database schema has already been sync'd.
   *
   * @param tp
   *          the table to synchronize
   * @param pushLocalTableLevelFiles
   *          true if local table-level files should be pushed up to the server.
   *          e.g. any html files on the device should be pushed to the server
   * @param pushLocalInstanceFiles
   *          if local media files associated with data rows should be pushed up
   *          to the server. The data files on the server are always pulled
   *          down.
   */
  private void synchronizeTableDataRowsAndAttachments(TableProperties tp) {
    DbTable table = DbTable.getDbTable(tp);
    // used to get the above from the ACTIVE store. if things go wonky, maybe
    // check to see if it was ACTIVE rather than SERVER for a reason. can't
    // think of one. one thing is that if it fails you'll see a table but won't
    // be able to open it, as there won't be any KVS stuff appropriate for it.
    boolean success = true;
    boolean instanceFileSuccess = true;
    // Prepare the tableResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    String tableId = tp.getTableId();
    TableResult tableResult = mUserResult.getTableResult(tableId);
    tableResult.setTableDisplayName(tp.getLocalizedDisplayName());
    if ( tableResult.getStatus() != Status.WORKING ) {
      // there was some sort of error...
      Log.e(TAG, "Skipping data sync - error in table schema or file verification step " + tableId);
      return;
    }

    boolean containsConflicts = false;

    beginTableTransaction(tp);
    try {
      {
        Log.i(TAG, "REST " + tableId);

        boolean once = true;
        while (once) {
          once = false;
          try {

            this.updateNotification(SyncProgressState.ROWS, R.string.verifying_table_schema_on_server, new Object[] { tp.getTableId() }, 0.0, false);

            // confirm that the local schema matches the one on the server.
            TableResource resource = synchronizer.getTableOrNull(tp.getTableId());

            if (resource == null) {
              // server does not know about it -- report that
              success = false;
              tableResult.setServerHadSchemaChanges(true);
              tableResult
                  .setMessage("Server no longer has table! Marking it as insert locally. Reset App Server to upload.");
              tableResult.setStatus(Status.TABLE_DOES_NOT_EXIST_ON_SERVER);
              return;
            }

            // test that the schemaETag matches
            // if it doesn't, the user MUST sync app-level files and
            // configuration
            // syncing at the app level will adjust/set the local table
            // properties
            // schemaETag to match that on the server.
            SyncTag syncTag = tp.getSyncTag();
            if (syncTag == null || !resource.getSchemaETag().equals(syncTag.getSchemaETag())) {
              // schemaETag is not identical
              success = false;
              tableResult.setServerHadSchemaChanges(true);
              tableResult
                  .setMessage("Server schemaETag differs! Sync app-level files and configuration in order to sync this table.");
              tableResult.setStatus(Status.TABLE_REQUIRES_APP_LEVEL_SYNC);
              return;
            }

            // //////////////////////////////////////////////////
            // //////////////////////////////////////////////////
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // and now sync the data rows...

            this.updateNotification(SyncProgressState.ROWS, R.string.getting_changed_rows_on_server, new Object[] { tp.getTableId() }, 5.0, false);

            IncomingRowModifications modification;
            try {
              modification = synchronizer.getUpdates(tp.getTableId(), tp.getSyncTag());
            } catch (Exception e) {
              String msg = e.getMessage();
              if (msg == null)
                msg = e.toString();
              tableResult.setMessage(msg);
              tableResult.setStatus(Status.EXCEPTION);
              return;
            }


            this.updateNotification(SyncProgressState.ROWS, R.string.anaylzing_row_changes, new Object[] { tp.getTableId() }, 7.0, false);

            /**************************
             * PART 2: UPDATE THE DATA
             **************************/
            Log.d(TAG, "updateDbFromServer setServerHadDataChanges(true)");
            tableResult.setServerHadDataChanges(modification.hasTableDataChanged());

            Map<String, SyncRow> changedServerRows = modification.getRows();

            // get all the rows in the data table -- we will iterate through
            // them all.
            UserTable localDataTable = table.rawSqlQuery(null, null, null, null, null, null);
            containsConflicts = localDataTable.hasConflictRows();

            if ( localDataTable.hasCheckpointRows() ) {
              tableResult.setMessage(context.getString(R.string.table_contains_checkpoints));
              tableResult.setStatus(Status.TABLE_CONTAINS_CHECKPOINTS);
              return;
            }
            // these are all the various actions we will need to take:

            // serverRow updated; no matching localRow
            List<FileSyncRow> rowsToInsertLocally = new ArrayList<FileSyncRow>();

            // serverRow updated; localRow SyncState is rest or
            // rest_pending_files
            List<FileSyncRow> rowsToUpdateLocally = new ArrayList<FileSyncRow>();

            // serverRow deleted; localRow SyncState is rest or
            // rest_pending_files
            List<FileSyncRow> rowsToDeleteLocally = new ArrayList<FileSyncRow>();

            // serverRow updated or deleted; localRow SyncState is not rest or
            // rest_pending_files
            List<FileSyncRow> rowsToMoveToConflictingLocally = new ArrayList<FileSyncRow>();

            // localRow SyncState.inserting no changes pulled from server
            List<SyncRow> rowsToInsertOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.updating no changes pulled from server
            List<SyncRow> rowsToUpdateOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.deleting no changes pulled from server
            List<SyncRow> rowsToDeleteOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.rest_pending_files no changes pulled from
            // server
            List<SyncRow> rowsToPushFileAttachments = new ArrayList<SyncRow>();

            // loop through the localRow table
            for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
              Row localRow = localDataTable.getRowAtIndex(i);
              String stateStr = localRow.getDataOrMetadataByElementKey(DataTableColumns.SYNC_STATE);
              SyncState state = SyncState.valueOf(stateStr);

              String rowId = localRow.getRowId();

              // see if there is a change to this row that we need to pull down
              // from the server.
              SyncRow serverRow = changedServerRows.get(rowId);

              if (serverRow == null) {
                // the local row wasn't impacted by a server change
                // see if this local row should be pushed to the server.
                if (state == SyncState.inserting) {
                  rowsToInsertOnServer.add(convertToSyncRow(tp, localRow));
                } else if (state == SyncState.updating) {
                  rowsToUpdateOnServer.add(convertToSyncRow(tp, localRow));
                } else if (state == SyncState.deleting) {
                  rowsToDeleteOnServer.add(convertToSyncRow(tp, localRow));
                } else if (state == SyncState.rest_pending_files) {
                  rowsToPushFileAttachments.add(convertToSyncRow(tp, localRow));
                }
                // otherwise, it is in the rest state or conflicting state
                // and nothing should be done with it...
                continue;
              }

              // OK -- the server is reporting a change (in serverRow) to the
              // localRow.
              // if the localRow is already in a conflicting state, determine
              // what its
              // ConflictType is. If the localRow holds the earlier server-side
              // change,
              // then skip and look at the next record.
              int localRowConflictTypeBeforeSync = -1;
              if (state == SyncState.conflicting) {
                // we need to remove the conflicting records that refer to the
                // prior state of the server
                String localRowConflictTypeBeforeSyncStr = localRow
                    .getDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE);
                localRowConflictTypeBeforeSync = Integer
                    .parseInt(localRowConflictTypeBeforeSyncStr);
                if (localRowConflictTypeBeforeSync == ConflictType.SERVER_DELETED_OLD_VALUES
                    || localRowConflictTypeBeforeSync == ConflictType.SERVER_UPDATED_UPDATED_VALUES) {
                  // This localRow holds the server values from a
                  // previously-identified conflict.
                  // Skip it -- we will clean up this copy later once we find
                  // the matching localRow
                  // that holds the locally-changed values that were in conflict
                  // with this earlier
                  // set of server values.
                  continue;
                }
              }

              // remove this server row from the map of changes reported by the
              // server.
              // the following decision tree will always place the row into one
              // of the
              // local action lists.
              changedServerRows.remove(rowId);

              // OK the record is either a simple local record or a local
              // conflict record
              if (state == SyncState.rest || state == SyncState.rest_pending_files) {
                // the server's change should be applied locally.
                //
                // the file attachments might be stale locally,
                // but those are dealt with separately.

                if (serverRow.isDeleted()) {
                  rowsToDeleteLocally.add(new FileSyncRow(serverRow,
                      convertToSyncRow(tp, localRow), (state == SyncState.rest_pending_files)));
                } else {
                  rowsToUpdateLocally.add(new FileSyncRow(serverRow,
                      convertToSyncRow(tp, localRow), (state == SyncState.rest_pending_files)));
                }
              } else if (serverRow.isDeleted()
                  && (state == SyncState.deleting || (state == SyncState.conflicting && localRowConflictTypeBeforeSync == ConflictType.LOCAL_DELETED_OLD_VALUES))) {
                // this occurs if
                // (1) a deleting request was never ACKed but it was performed
                // on the server.
                // (2) if there is an unresolved conflict held locally with the
                // local action
                // being to delete the record, and the prior server state being
                // a value
                // change, but the newly sync'd state now reflects a deletion by
                // another
                // party.
                //

                // no need to worry about server conflict records.
                // any server conflict rows will be deleted during the delete
                // step
                rowsToDeleteLocally.add(new FileSyncRow(serverRow, convertToSyncRow(tp, localRow),
                    false));
              } else {
                // SyncState.deleting and server is not deleting
                // SyncState.inserting and record exists on server
                // SyncState.updating and new change on server
                // SyncState.conflicting and new change on server

                // no need to worry about server conflict records.
                // any server conflict rows will be cleaned up during the
                // update of the conflicting state.

                // figure out what the localRow conflict type should be...
                Integer localRowConflictType;
                if (state == SyncState.updating) {
                  // SyncState.updating and new change on server
                  localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
                  Log.i(TAG, "local row was in sync state UPDATING, changing to "
                      + "CONFLICT and setting conflict type to: " + localRowConflictType);
                } else if (state == SyncState.inserting) {
                  // SyncState.inserting and record exists on server
                  // The 'inserting' case occurs if an insert is never ACKed but
                  // completes successfully on the server.
                  localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
                  Log.i(TAG, "local row was in sync state INSERTING, changing to "
                      + "CONFLICT and setting conflict type to: " + localRowConflictType);
                } else if (state == SyncState.deleting) {
                  // SyncState.deleting and server is not deleting
                  localRowConflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
                  Log.i(TAG, "local row was in sync state DELETING, changing to "
                      + "CONFLICT and updating conflict type to: " + localRowConflictType);
                } else if (state == SyncState.conflicting) {
                  // SyncState.conflicting and new change on server
                  // leave the local conflict type unchanged (retrieve it and
                  // use it).
                  localRowConflictType = localRowConflictTypeBeforeSync;
                  Log.i(TAG, "local row was in sync state CONFLICTING, leaving as "
                      + "CONFLICTING and leaving conflict type unchanged as: "
                      + localRowConflictTypeBeforeSync);
                } else {
                  throw new IllegalStateException("Unexpected state encountered");
                }
                rowsToMoveToConflictingLocally.add(new FileSyncRow(serverRow, convertToSyncRow(tp,
                    localRow), false, localRowConflictType));
              }
            }

            // Now, go through the remaining serverRows in the rows map. That
            // map now contains only row changes that don't affect any existing
            // localRow. If the server change is not a row-deletion / revoke-row
            // action,
            // then insert the serverRow locally.
            for (SyncRow serverRow : changedServerRows.values()) {
              boolean isDeleted = serverRow.isDeleted();
              if (!isDeleted) {
                rowsToInsertLocally.add(new FileSyncRow(serverRow, null, false));
              }
            }

            //
            // OK we have captured the local inserting, locally updating,
            // locally deleting and conflicting actions. And we know
            // the changes for the server. Determine the per-row percentage
            // for applying all these changes

            int totalChange = rowsToInsertLocally.size() +
                rowsToUpdateLocally.size() + rowsToDeleteLocally.size() +
                rowsToMoveToConflictingLocally.size() +
                rowsToInsertOnServer.size() +
                rowsToUpdateOnServer.size() +
                rowsToDeleteOnServer.size() +
                rowsToPushFileAttachments.size();

            containsConflicts = containsConflicts || !rowsToMoveToConflictingLocally.isEmpty();

            perRowIncrement = 90.0 / ((double) (totalChange + 1));
            rowsProcessed = 0;

            // i.e., we have created entries in the various action lists
            // for all the actions we should take.

            // ///////////////////////////////////////////////////
            // / PERFORM LOCAL DATABASE CHANGES
            // / PERFORM LOCAL DATABASE CHANGES
            // / PERFORM LOCAL DATABASE CHANGES
            // / PERFORM LOCAL DATABASE CHANGES
            // / PERFORM LOCAL DATABASE CHANGES

            success = deleteRowsInDb(resource, tp, table, rowsToDeleteLocally, tableResult);
            if (!insertRowsInDb(resource, tp, table, rowsToInsertLocally, tableResult)) {
              instanceFileSuccess = false;
            }
            boolean[] results = updateRowsInDb(resource, tp, table, rowsToUpdateLocally, tableResult);
            if ( !results[0] ) {
              success = false;
            }
            if ( !results[1] ) {
              instanceFileSuccess = false;
            }
            if ( !conflictRowsInDb(resource, tp, table, rowsToMoveToConflictingLocally, tableResult)) {
              instanceFileSuccess = false;
            }

            // If we made it here and there was data, then we successfully
            // updated the
            // data from the server.
            if (changedServerRows.size() > 0) {
              tableResult.setPulledServerData(success);
            }

            // TODO: fix this for rest_pending_files
            // We likely need to relax this constraint on the
            // server?

            // We have to set this synctag here so that the server knows we saw
            // its
            // changes. Otherwise it won't let us put up new information.
            if (success) {
              tp.setSyncTag(modification.getTableSyncTag());
            }

            // /////////////////////////////////////
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES

            if (rowsToInsertOnServer.size() != 0 || rowsToUpdateOnServer.size() != 0
                || rowsToDeleteOnServer.size() != 0) {
              if (tableResult.hadLocalDataChanges()) {
                Log.e(TAG, "synchronizeTableRest hadLocalDataChanges() returned "
                    + "true, and we're about to set it to true again. Odd.");
              }
              tableResult.setHadLocalDataChanges(true);
            }

            // push the changes up to the server
            boolean serverSuccess = false;
            try {
              SyncTag revisedTag = tp.getSyncTag();

              // idempotent interface means that the interactions
              // for inserts and for updates are identical.
              int count = 0;
              List<SyncRow> allUpsertRows = new ArrayList<SyncRow>();
              allUpsertRows.addAll(rowsToInsertOnServer);
              allUpsertRows.addAll(rowsToUpdateOnServer);
              for (SyncRow syncRow : allUpsertRows) {
                RowModification rm = synchronizer.insertOrUpdateRow(tableId, revisedTag, syncRow);

                ContentValues values = new ContentValues();
                values.put(DataTableColumns.ROW_ETAG, rm.getRowETag());
                values.put(DataTableColumns.SYNC_STATE, SyncState.rest_pending_files.name());
                table.actualUpdateRowByRowId(rm.getRowId(), values);
                tableResult.incServerUpserts();

                boolean outcome = synchronizer.putFileAttachments(resource.getInstanceFilesUri(), tableId, syncRow);
                if (outcome) {
                  // move to rest state
                  values.clear();
                  values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
                  table.actualUpdateRowByRowId(syncRow.getRowId(), values);
                } else {
                  instanceFileSuccess = false;
                }

                revisedTag = rm.getTableSyncTag();
                if (success) {
                  tp.setSyncTag(revisedTag);
                }
                ++count;
                ++rowsProcessed;
                if ( rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0 ) {
                  this.updateNotification(SyncProgressState.ROWS, R.string.upserting_server_row,
                      new Object[] { tp.getTableId(), count, allUpsertRows.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
                }
              }

              count = 0;
              for (SyncRow syncRow : rowsToDeleteOnServer) {
                RowModification rm = synchronizer.deleteRow(tableId, revisedTag, syncRow);
                table.deleteRowActual(rm.getRowId());
                tableResult.incServerDeletes();
                revisedTag = rm.getTableSyncTag();
                if (success) {
                  tp.setSyncTag(revisedTag);
                }
                ++count;
                ++rowsProcessed;
                if ( rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0 ) {
                  this.updateNotification(SyncProgressState.ROWS, R.string.deleting_server_row,
                      new Object[] { tp.getTableId(), count, rowsToDeleteOnServer.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
                }
              }

              // And try to push the file attachments...
              count = 0;
              for (SyncRow syncRow : rowsToPushFileAttachments) {
                boolean outcome = synchronizer.putFileAttachments(resource.getInstanceFilesUri(), tableId, syncRow);
                if (outcome) {
                  outcome = synchronizer.getFileAttachments(resource.getInstanceFilesUri(), tableId, syncRow, true);
                  if (outcome) {
                    ContentValues values = new ContentValues();
                    values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
                    table.actualUpdateRowByRowId(syncRow.getRowId(), values);
                  }
                }
                if ( !outcome ) {
                  instanceFileSuccess = false;
                }
                tableResult.incLocalAttachmentRetries();
                ++count;
                ++rowsProcessed;
                if ( rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0 ) {
                  this.updateNotification(SyncProgressState.ROWS, R.string.uploading_attachments_server_row,
                      new Object[] { tp.getTableId(), count, rowsToPushFileAttachments.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
                }
              }

              // And now update that we've pushed our changes to the server.
              tableResult.setPushedLocalData(true);
              serverSuccess = true;
            } catch (IOException e) {
              ioException("synchronizeTableRest", tp, e, tableResult);
              serverSuccess = false;
            } catch (Exception e) {
              exception("synchronizeTableRest", tp, e, tableResult);
              serverSuccess = false;
            }
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // //////////////////////////////////////////////////////////
            // //////////////////////////////////////////////////////////

            success = success && serverSuccess;

          } catch (ResourceAccessException e) {
            resourceAccessException("synchronizeTableRest--nonMediaFiles", tp, e, tableResult);
            Log.e(TAG, "[synchronizeTableRest] error synchronizing table files");
            success = false;
          } catch (Exception e) {
            exception("synchronizeTableRest--nonMediaFiles", tp, e, tableResult);
            Log.e(TAG, "[synchronizeTableRest] error synchronizing table files");
            success = false;
          }
        }
      }

      // It is possible the table properties changed. Refresh just in case.
      tp = TableProperties.refreshTablePropertiesForTable(context, appName, tp.getTableId());
      if ( tp.getDbTableName() == null ) {
        tp = null;
      }
      if (success && tp != null) // null in case we deleted the tp.
        tp.setLastSyncTime(du.formatNowForDb());
    } finally {
      endTableTransaction(tp, success);
      // Here we also want to add the TableResult to the value.
      if (success) {
        // Then we should have updated the db and shouldn't have set the
        // TableResult to be exception.
        if (tableResult.getStatus() != Status.WORKING) {
          Log.e(TAG, "tableResult status for table: " + tp.getDbTableName() +
              " was " + tableResult.getStatus().name() +
              ", and yet success returned true. This shouldn't be possible.");
        } else {
          if ( containsConflicts ) {
            tableResult.setStatus(Status.TABLE_CONTAINS_CONFLICTS);
            this.updateNotification(SyncProgressState.ROWS, R.string.table_data_sync_with_conflicts, new Object[] {tp.getTableId()}, 100.0, false);
          } else if ( !instanceFileSuccess ) {
            tableResult.setStatus(Status.TABLE_PENDING_ATTACHMENTS);
            this.updateNotification(SyncProgressState.ROWS, R.string.table_data_sync_pending_attachments, new Object[] {tp.getTableId()}, 100.0, false);
          } else {
            tableResult.setStatus(Status.SUCCESS);
            this.updateNotification(SyncProgressState.ROWS, R.string.table_data_sync_complete, new Object[] {tp.getTableId()}, 100.0, false);
          }
        }
      }
    }
  }

  static final class FileSyncRow {
    final SyncRow serverRow;
    final SyncRow localRow;
    final boolean isRestPendingFiles;
    final int localRowConflictType;

    FileSyncRow(SyncRow serverRow, SyncRow localRow, boolean isRestPendingFiles) {
      this.serverRow = serverRow;
      this.localRow = localRow;
      this.isRestPendingFiles = isRestPendingFiles;
      this.localRowConflictType = -1;
    }

    FileSyncRow(SyncRow serverRow, SyncRow localRow, boolean isRestPendingFiles,
        int localRowConflictType) {
      this.serverRow = serverRow;
      this.localRow = localRow;
      this.isRestPendingFiles = isRestPendingFiles;
      this.localRowConflictType = localRowConflictType;
    }
  };

  private void resourceAccessException(String method, TableProperties tp,
      ResourceAccessException e, TableResult tableResult) {
    Log.e(TAG,
        String.format("ResourceAccessException in %s for table: %s", method, tp.getTableId()), e);
    tableResult.setStatus(Status.AUTH_EXCEPTION);
    tableResult.setMessage(e.getMessage());
  }

  private void ioException(String method, TableProperties tp, IOException e, TableResult tableResult) {
    Log.e(TAG, String.format("IOException in %s for table: %s", method, tp.getTableId()), e);
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(e.getMessage());
  }

  private void exception(String method, TableProperties tp, Exception e, TableResult tableResult) {
    Log.e(TAG, String.format("Unexpected exception in %s on table: %s", method, tp.getTableId()), e);
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(e.getMessage());
  }

  private boolean conflictRowsInDb(TableResource resource, TableProperties tp, DbTable table, List<FileSyncRow> changes, TableResult tableResult)
      throws ResourceAccessException {

    boolean fileSuccess = true;
    int count = 0;
    for (FileSyncRow change : changes) {
      SyncRow serverRow = change.serverRow;
      Log.i(TAG,
          "conflicting row, id=" + serverRow.getRowId() + " rowETag=" + serverRow.getRowETag());
      ContentValues values = new ContentValues();

      // delete the old server-values conflicting row if it exists
      String whereClause = String.format("%s = ? AND %s = ? AND %s IN " + "( ?, ? )",
          DataTableColumns.ID, DataTableColumns.SYNC_STATE, DataTableColumns.CONFLICT_TYPE);
      String[] whereArgs = { serverRow.getRowId(), SyncState.conflicting.name(),
          String.valueOf(ConflictType.SERVER_DELETED_OLD_VALUES),
          String.valueOf(ConflictType.SERVER_UPDATED_UPDATED_VALUES) };
      table.deleteRowActual(whereClause, whereArgs);

      // update existing localRow

      // the localRow conflict type was determined when the
      // change was added to the changes list.
      Integer localRowConflictType = change.localRowConflictType;

      // Determine the type of change that occurred on the server.
      int serverRowConflictType;
      if (serverRow.isDeleted()) {
        serverRowConflictType = ConflictType.SERVER_DELETED_OLD_VALUES;
      } else {
        serverRowConflictType = ConflictType.SERVER_UPDATED_UPDATED_VALUES;
      }

      if (serverRowConflictType == ConflictType.SERVER_DELETED_OLD_VALUES
          && localRowConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES) {

        // special case -- the server and local rows are both being deleted
        // just delete them!
        table.deleteRowActual(serverRow.getRowId());
        tableResult.incLocalDeletes();
      } else {
        // update the localRow to be conflicting
        values.put(DataTableColumns.ID, serverRow.getRowId());
        values.put(DataTableColumns.SYNC_STATE, SyncState.conflicting.name());
        values.put(DataTableColumns.CONFLICT_TYPE, localRowConflictType);
        table.actualUpdateRowByRowId(serverRow.getRowId(), values);

        // set up to insert the conflicting row from the server
        for (Entry<String, String> entry : serverRow.getValues().entrySet()) {
          String colName = entry.getKey();
          values.put(colName, entry.getValue());
        }

        // insert conflicting server row
        values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
        values.put(DataTableColumns.SYNC_STATE, SyncState.conflicting.name());
        values.put(DataTableColumns.CONFLICT_TYPE, serverRowConflictType);
        values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
        values.put(DataTableColumns.LOCALE, serverRow.getLocale());
        values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
        values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());
        Scope.Type type = serverRow.getFilterScope().getType();
        values.put(DataTableColumns.FILTER_TYPE,
            (type == null) ? Scope.Type.DEFAULT.name() : type.name());
        values.put(DataTableColumns.FILTER_VALUE, serverRow.getFilterScope().getValue());
        table.actualAddRow(values);

        // We're going to check our representation invariant here. A local and
        // a server version of the row should only ever be updating/updating,
        // deleted/updating, or updating/deleted. Anything else and we're in
        // trouble.
        if (localRowConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES
            && serverRowConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES) {
          Log.e(TAG, "local row conflict type is local_deleted, but server "
              + "row conflict_type is not server_udpated. These states must"
              + " go together, something went wrong.");
        } else if (localRowConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES) {
          Log.e(TAG, "localRowConflictType was not local_deleted or "
              + "local_updated! this is an error. local conflict type: " + localRowConflictType
              + ", server conflict type: " + serverRowConflictType);
        }

        tableResult.incLocalConflicts();

        // ensure we have the file attachments for the conflicting row
        // it is OK if we can't get them, but they may be useful for
        // reconciliation
        boolean outcome = synchronizer.getFileAttachments(resource.getInstanceFilesUri(), tp.getTableId(), serverRow, false);
        if (!outcome) {
          // we don't do anything on failure -- just log a warning.
          // we need to leave the sync state as conflicting.
          fileSuccess = false;
          Log.w(TAG, "Unable to fetch file attachments from conflicting row on server");
        }
      }
      ++count;
      ++rowsProcessed;
      if ( rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0 ) {
        this.updateNotification(SyncProgressState.ROWS, R.string.marking_conflicting_local_row,
            new Object[] { tp.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
      }
    }
    return fileSuccess;
  }

  private boolean insertRowsInDb(TableResource resource, TableProperties tp, DbTable table, List<FileSyncRow> changes, TableResult tableResult) throws ResourceAccessException {
    boolean fileSuccess = true;
    int count = 0;
    for (FileSyncRow change : changes) {
      SyncRow serverRow = change.serverRow;
      ContentValues values = new ContentValues();

      values.put(DataTableColumns.ID, serverRow.getRowId());
      values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
      values.put(DataTableColumns.SYNC_STATE, SyncState.rest_pending_files.name());
      values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
      values.put(DataTableColumns.LOCALE, serverRow.getLocale());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
      values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());

      for (Entry<String, String> entry : serverRow.getValues().entrySet()) {
        String colName = entry.getKey();
        values.put(colName, entry.getValue());
      }

      table.actualAddRow(values);
      tableResult.incLocalInserts();

      // ensure we have the file attachments for the inserted row
      boolean outcome = synchronizer.getFileAttachments(resource.getInstanceFilesUri(), tp.getTableId(), serverRow, true);
      if (outcome) {
        // move to rest state
        values.clear();
        values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
        table.actualUpdateRowByRowId(serverRow.getRowId(), values);
      } else {
        fileSuccess = false;
      }
      ++count;
      ++rowsProcessed;
      if ( rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0 ) {
        this.updateNotification(SyncProgressState.ROWS, R.string.inserting_local_row,
            new Object[] { tp.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
      }
    }
    return fileSuccess;
  }

  private boolean[] updateRowsInDb(TableResource resource, TableProperties tp, DbTable table, List<FileSyncRow> changes, TableResult tableResult) throws ResourceAccessException {
    boolean success = true;
    boolean fileSuccess = true;

    int count = 0;
    for (FileSyncRow change : changes) {
      // if the localRow sync state was rest_pending_files,
      // ensure that all those files are uploaded before
      // we update the row. This ensures that all attachments
      // are saved before we revise the local row value.
      boolean outcome = true;
      if (change.isRestPendingFiles) {
        // we need to push our changes to the server first...
        outcome = synchronizer.putFileAttachments(resource.getInstanceFilesUri(), tp.getTableId(), change.localRow);
      }

      if (!outcome) {
        // leave this row stale because we haven't been able to
        // finish the post of the older row's file.
        success = false;
        fileSuccess = false;
      } else {
        // OK we have the files sync'd (if we needed to do that).

        // update the row from the changes on the server
        SyncRow serverRow = change.serverRow;
        ContentValues values = new ContentValues();

        values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
        values.put(DataTableColumns.SYNC_STATE, SyncState.rest_pending_files.name());
        values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
        values.put(DataTableColumns.LOCALE, serverRow.getLocale());
        values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
        values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());

        for (Entry<String, String> entry : serverRow.getValues().entrySet()) {
          String colName = entry.getKey();
          values.put(colName, entry.getValue());
        }

        table.actualUpdateRowByRowId(serverRow.getRowId(), values);
        tableResult.incLocalUpdates();

        // and try to get the file attachments for the row
        outcome = synchronizer.getFileAttachments(resource.getInstanceFilesUri(), tp.getTableId(), serverRow, true);
        if (outcome) {
          // move to rest state
          values.clear();
          values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
          table.actualUpdateRowByRowId(serverRow.getRowId(), values);
        } else {
          fileSuccess = false;
        }
        // otherwise, leave in rest_pending_files state.
      }
      ++count;
      ++rowsProcessed;
      if ( rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0 ) {
        this.updateNotification(SyncProgressState.ROWS, R.string.updating_local_row,
            new Object[] { tp.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
      }
    }
    boolean[] results = {success, fileSuccess};
    return results;
  }

  private boolean deleteRowsInDb(TableResource resource, TableProperties tp, DbTable table, List<FileSyncRow> changes, TableResult tableResult)
      throws IOException {
    int count = 0;
    boolean deletesAllSuccessful = true;
    for (FileSyncRow change : changes) {
      if (change.isRestPendingFiles) {
        boolean outcome = synchronizer.putFileAttachments(resource.getInstanceFilesUri(), tp.getTableId(), change.localRow);
        if (outcome) {
          table.deleteRowActual(change.serverRow.getRowId());
          tableResult.incLocalDeletes();
        } else {
          deletesAllSuccessful = false;
        }
      }
      ++count;
      ++rowsProcessed;
      if ( rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0 ) {
        this.updateNotification(SyncProgressState.ROWS, R.string.deleting_local_row,
            new Object[] { tp.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
      }
    }
    return deletesAllSuccessful;
  }

  private SyncRow convertToSyncRow(TableProperties tp, Row localRow) {
    String rowId = localRow.getRowId();
    String rowETag = localRow.getDataOrMetadataByElementKey(DataTableColumns.ROW_ETAG);
    Map<String, String> values = new HashMap<String, String>();

    for (ColumnProperties cp : tp.getAllColumns().values()) {
      if (cp.isUnitOfRetention()) {
        String elementKey = cp.getElementKey();
        values.put(elementKey, localRow.getDataOrMetadataByElementKey(elementKey));
      }
    }
    SyncRow syncRow = new SyncRow(rowId, rowETag, false,
        localRow.getDataOrMetadataByElementKey(DataTableColumns.FORM_ID),
        localRow.getDataOrMetadataByElementKey(DataTableColumns.LOCALE),
        localRow.getDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TYPE),
        localRow.getDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TIMESTAMP),
        localRow.getDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_CREATOR), Scope.asScope(
            localRow.getDataOrMetadataByElementKey(DataTableColumns.FILTER_TYPE),
            localRow.getDataOrMetadataByElementKey(DataTableColumns.FILTER_VALUE)), values);
    return syncRow;
  }

  private void beginTableTransaction(TableProperties tp) {
    tp.setTransactioning(true);
  }

  private void endTableTransaction(TableProperties tp, boolean success) {
    tp.setTransactioning(false);
  }

  /**
   * Update the database to reflect the new structure.
   * <p>
   * This should be called when downloading a table from the server, which is
   * why the syncTag is separate. TODO: pass the db around rather than dbh so we
   * can do this transactionally
   *
   * @param definitionResource
   * @param syncTag
   *          the syncTag belonging to the modification from which you acquired
   *          the {@link TableDefinitionResource}.
   * @return the new {@link TableProperties} for the table.
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   * @throws SchemaMismatchException
   */
  @SuppressWarnings("unchecked")
  private TableProperties addTableFromDefinitionResource(
      TableDefinitionResource definitionResource, TableProperties tp) throws JsonParseException,
      JsonMappingException, IOException, SchemaMismatchException {
    if (tp == null) {
      tp = TableProperties.addTable(context, appName, definitionResource.getTableId(),
          definitionResource.getTableId(), definitionResource.getTableId());
      for (Column col : definitionResource.getColumns()) {
        // TODO: We aren't handling types correctly here. Need to have a mapping
        // on the server as well so that you can pull down the right thing.
        // TODO: add an addcolumn method to allow setting all of the
        // dbdefinition
        // fields.
        List<String> listChildElementKeys = null;
        String lek = col.getListChildElementKeys();
        if (lek != null && lek.length() != 0) {
          listChildElementKeys = mapper.readValue(lek, List.class);
        }
        tp.addColumn(col.getElementKey(), col.getElementKey(), col.getElementName(),
            ColumnType.valueOf(col.getElementType()), listChildElementKeys,
            DataHelper.intToBool(col.getIsUnitOfRetention()));
      }
      tp.setSyncTag(new SyncTag(null, definitionResource.getSchemaETag()));
    } else {
      // see if the server copy matches our local schema
      for (Column col : definitionResource.getColumns()) {
        List<String> listChildElementKeys;
        String lek = col.getListChildElementKeys();
        if (lek != null && lek.length() != 0) {
          listChildElementKeys = mapper.readValue(lek, List.class);
        } else {
          listChildElementKeys = new ArrayList<String>();
        }
        ColumnProperties cp = tp.getColumnByElementKey(col.getElementKey());
        if (cp == null) {
          // we can support modifying of schema via adding of columns
          tp.addColumn(col.getElementKey(), col.getElementKey(), col.getElementName(),
              ColumnType.valueOf(col.getElementType()), listChildElementKeys,
              DataHelper.intToBool(col.getIsUnitOfRetention()));
        } else {
          List<String> cpListChildElementKeys = cp.getListChildElementKeys();
          if (cpListChildElementKeys == null) {
            cpListChildElementKeys = new ArrayList<String>();
          }
          if (!((cp.getElementName() == col.getElementName() || ((cp.getElementName() != null) && cp
              .getElementName().equals(col.getElementName())))
              && cp.isUnitOfRetention() == DataHelper.intToBool(col.getIsUnitOfRetention())
              && cpListChildElementKeys.size() == listChildElementKeys.size() && cpListChildElementKeys
                .containsAll(listChildElementKeys))) {
            throw new SchemaMismatchException("Server schema differs from local schema");
          } else if (!cp.getColumnType().equals(ColumnType.valueOf(col.getElementType()))) {
            // we have a column datatype change.
            // we should be able to handle this for simple types (unknown ->
            // text
            // or text -> integer)
            throw new SchemaMismatchException(
                "Server schema differs from local schema (column datatype change)");
          }
        }
      }

      SyncTag syncTag = tp.getSyncTag();
      if (syncTag == null || syncTag.getSchemaETag() == null || !syncTag.getSchemaETag().equals(definitionResource.getSchemaETag())) {
        // server has changed its schema
        // change row sync and conflict status to handle new server schema.
        // Clean up this table and set the dataETag to null.
        DbTable table = DbTable.getDbTable(tp);
        table.changeDataRowsToInsertingState();
        // and update to the new schemaETag, but clear our dataETag
        // so that all data rows sync.
        tp.setSyncTag(new SyncTag(null, definitionResource.getSchemaETag()));
      }
    }
    return tp;
  }

  /**
   * Return a list of {@link Column} objects (representing the column
   * definition) for each of the columns associated with this table.
   *
   * @param tp
   * @return
   */
  private ArrayList<Column> getColumnsForTable(TableProperties tp) {
    ArrayList<Column> columns = new ArrayList<Column>();
    for (ColumnProperties cp : tp.getAllColumns().values()) {
      String elementKey = cp.getElementKey();
      String elementName = cp.getElementName();
      ColumnType colType = cp.getColumnType();
      List<String> listChildrenElements = cp.getListChildElementKeys();
      int isUnitOfRetention = DataHelper.boolToInt(cp.isUnitOfRetention());
      String listChildElementKeysStr = null;
      try {
        listChildElementKeysStr = mapper.writeValueAsString(listChildrenElements);
      } catch (JsonGenerationException e) {
        Log.e(TAG, "problem parsing json list entry during sync");
        e.printStackTrace();
      } catch (JsonMappingException e) {
        Log.e(TAG, "problem mapping json list entry during sync");
        e.printStackTrace();
      } catch (IOException e) {
        Log.e(TAG, "i/o exception with json list entry during sync");
        e.printStackTrace();
      }
      // Column c = new Column(tp.getTableId(), elementKey, elementName,
      // colType.name(), listChildElementKeysStr,
      // (isUnitOfRetention != 0), joinsStr);
      Column c = new Column(tp.getTableId(), elementKey, elementName, colType.name(),
          listChildElementKeysStr, (isUnitOfRetention != 0));
      columns.add(c);
    }
    return columns;
  }
}
