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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
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
import org.opendatakit.common.android.data.TableProperties;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.UserTable.Row;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.sync.aggregate.SyncTag;
import org.opendatakit.common.android.sync.exceptions.SchemaMismatchException;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utils.CsvUtil;
import org.opendatakit.common.android.utils.DataUtil;
import org.opendatakit.sync.Synchronizer.OnTablePropertiesChanged;
import org.opendatakit.sync.TableResult.Status;
import org.springframework.web.client.ResourceAccessException;

import android.content.ContentValues;
import android.content.Context;
import android.content.SyncResult;
import android.util.Log;

/**
 * SyncProcessor implements the cloud synchronization logic for Tables.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class SyncProcessor {

  private static final String TAG = SyncProcessor.class.getSimpleName();

  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
  }

  private final Context context;
  private final String appName;
  private final DataUtil du;
  private final SyncResult syncResult;
  private final Synchronizer synchronizer;
  /**
   * The results of the synchronization that we will pass back to the user. Note
   * that this is NOT the same as the {@link SyncResult} object, which is used
   * to inform the android SyncAdapter how the sync process has gone.
   */
  private final SynchronizationResult mUserResult;

  public SyncProcessor(Context context, String appName, Synchronizer synchronizer, SyncResult syncResult) {
    this.context = context;
   this.appName = appName;
    this.du = new DataUtil(Locale.ENGLISH, TimeZone.getDefault());;
    this.syncResult = syncResult;
    this.synchronizer = synchronizer;
    this.mUserResult = new SynchronizationResult();
  }

  public List<TableResult> getTableResults() {
    return mUserResult.getTableResults();
  }

  /**
   * Synchronize all app-level files and all data table schemas and table-level files.
   *
   * This synchronization sets the stage for data row synchronization.  The two
   * modes are either to pull all this configuration down from the server and
   * enforce that the client contain all files and tables on the server or to
   * enforce that the server contains all files and tables that are on the client.
   *
   * When pulling down (the normal mode of operation), we reload the local properties
   * from the tables/tableId/properties.csv that has been pulled down from the server.
   *
   * This does not process zip files; it is unclear whether we should do anything for
   * those or just leave them as zip files locally.
   */
  public SynchronizationResult synchronizeConfigurationAndContent(boolean pushToServer) {
    Log.i(TAG, "entered synchronizeConfigurationAndContent()");
    ODKFileUtils.assertDirectoryStructure(appName);

    // First we're going to synchronize the app level files.
    try {
      synchronizer.syncAppLevelFiles(pushToServer);
    } catch (ResourceAccessException e) {
      // TODO: update a synchronization result to report back to them as well.
      Log.e(TAG, "[synchronizeConfigurationAndContent] error trying to synchronize app-level files.");
      e.printStackTrace();
    }

    TableProperties[] tps;
    tps = TableProperties.getTablePropertiesForAll(context, appName);
    if ( pushToServer ) {
      /////////////////////////////////////////////
      /// UPDATE SERVER CONTENT
      /// UPDATE SERVER CONTENT
      /// UPDATE SERVER CONTENT
      /// UPDATE SERVER CONTENT
      /// UPDATE SERVER CONTENT
      for (TableProperties tp : tps) {
        Log.i(TAG, "[synchronizeConfigurationAndContent] synchronizing table " + tp.getTableId());
        synchronizeTableConfigurationAndContent(tp, true);
      }
    } else {
      ////////////////////////////////////////////
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // MIMIC SERVER CONTENT
      // get tables (tableId -> schemaETag) from server
      List<TableResource> tables = new ArrayList<TableResource>();
      try {
         tables = synchronizer.getTables();
         if ( tables == null ) {
           tables = new ArrayList<TableResource>();
         }
      } catch (IOException e) {
         Log.i(TAG, "[synchronizeConfigurationAndContent] Could not retrieve table list", e);
         return null;
      } catch (Exception e) {
         Log.e(TAG, "[synchronizeConfigurationAndContent] Unexpected exception getting table list", e);
         return null;
      }

      List<TableProperties> toDelete = new ArrayList<TableProperties>();
      Collections.addAll(toDelete, tps);
      for ( TableResource table : tables ) {
        String tableId = table.getTableId();
        for (TableProperties p : tps) {
           if (p.getTableId().equals(tableId)) {
              toDelete.remove(p);
              break;
           }
        }

        TableResource tr;
        try {
           tr = synchronizer.getTable(tableId);
        } catch (IOException e) {
           // TODO report failure properly
           e.printStackTrace();
           return null;
        }

        TableProperties tp;
        try {
            TableDefinitionResource definitionResource = synchronizer
                .getTableDefinition(tr.getDefinitionUri());

            tp = addTableFromDefinitionResource(definitionResource);
        } catch (JsonParseException e) {
           // TODO report failure properly
           e.printStackTrace();
           return null;
        } catch (JsonMappingException e) {
           // TODO report failure properly
           e.printStackTrace();
           return null;
        } catch (IOException e) {
           // TODO report failure properly
           e.printStackTrace();
           return null;
        } catch (SchemaMismatchException e) {
           // TODO Auto-generated catch block
           e.printStackTrace();
           return null;
        }

        // Sync the local media files with the server if the table
        // existed locally before we attempted downloading it.

        synchronizeTableConfigurationAndContent(tp, false);
      }

      // and now loop through the ones to delete...
      for ( TableProperties tp : toDelete ) {
        tp.deleteTable();
      }

    }
    return mUserResult;
  }

  /**
   * Synchronize the table represented by the given TableProperties with the
   * cloud.
   * <p>
   * Note that if the db changes under you when calling this method, the tp
   * parameter will become out of date. It should be refreshed after calling
   * this method.
   * <p>
   * This method does NOT synchronize the application files. This means that if
   * any html files downloaded require the {@link TableFileUtils#DIR_FRAMEWORK}
   * directory, for instance, the caller must ensure that the app files are
   * synchronized as well.
   *
   * @param tp
   *          the table to synchronize
   * @param pushLocalTableLevelFiles
   *          true if local table-level files should be pushed up to the server.
   *          e.g. any html files on the device should be pushed to the server
   * @param pushLocalInstanceFiles
   *          if local media files associated with data rows should be pushed
   *          up to the server. The data files on the server are always pulled down.
   */
  private void synchronizeTableConfigurationAndContent(TableProperties tp,
                               boolean pushLocalTableLevelFiles) {

    // used to get the above from the ACTIVE store. if things go wonky, maybe
    // check to see if it was ACTIVE rather than SERVER for a reason. can't
    // think of one. one thing is that if it fails you'll see a table but won't
    // be able to open it, as there won't be any KVS stuff appropriate for it.
    boolean success = false;
    // Prepare the tableResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    TableResult tableResult = new TableResult(tp.getLocalizedDisplayName(), tp.getTableId());
    beginTableTransaction(tp);
    try {
        // presume success...
        tableResult.setStatus(Status.SUCCESS);

        // Confirm that the local schema matches that on the server...
        // If we are pushing to the server, create it on the server.

        // retrieve updates TODO: after adding editing a row and a color rule, then
        // synching, then copying the kvs into the server set and synching, this
        // returned true that tp had changed and that i had a new sync row (the old
        // row). this shouldn't do that.
        TableResource resource = synchronizer.getTableOrNull(tp.getTableId());

        if ( resource == null ) {

          if ( !pushLocalTableLevelFiles ) {
            // the table on the server is missing. Need to ask user what to do...
            tableResult.setServerHadSchemaChanges(true);
            tableResult.setMessage("Server no longer has table! Marking it as insert locally. Reset App Server to upload.");
            tableResult.setStatus(Status.TABLE_DOES_NOT_EXIST_ON_SERVER);
            return;
          }

          // the insert of the table was incomplete -- try again

          // we are creating data on the server
          // change our 'rest' state rows into 'inserting' rows
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
            resource = synchronizer.createTable(tp.getTableId(), newSyncTag,
                                      getColumnsForTable(tp));
          } catch (Exception e) {
            e.printStackTrace();
            String msg = e.getMessage();
            if ( msg == null ) msg = e.toString();
            tableResult.setMessage(msg);
            tableResult.setStatus(Status.EXCEPTION);
            return;
          }

          SyncTag syncTag = new SyncTag(null, resource.getSchemaETag());
          tp.setSyncTag(syncTag);

          // refresh the resource
          try {
            resource = synchronizer.getTableOrNull(tp.getTableId());
          } catch (Exception e) {
            e.printStackTrace();
            String msg = e.getMessage();
            if ( msg == null ) msg = e.toString();
            tableResult.setMessage(msg);
            tableResult.setStatus(Status.EXCEPTION);
            return;
          }

          if ( resource == null ) {
            tableResult.setMessage("Unexpected error -- table should have been created!");
            tableResult.setStatus(Status.FAILURE);
            return;
          }
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
        if (syncTag == null ||
            !resource.getSchemaETag().equals(syncTag.getSchemaETag()) ) {
          Log.d(TAG, "updateDbFromServer setServerHadSchemaChanges(true)");
          tableResult.setServerHadSchemaChanges(true);

          // fetch the table definition
          TableDefinitionResource definitionResource;
          try {
            definitionResource = synchronizer.getTableDefinition(resource.getDefinitionUri());
          } catch (Exception e) {
            e.printStackTrace();
            String msg = e.getMessage();
            if ( msg == null ) msg = e.toString();
            tableResult.setMessage(msg);
            tableResult.setStatus(Status.EXCEPTION);
            return;
          }

          // record that we have pulled it
          tableResult.setPulledServerSchema(true);
          try {
            // apply changes
            // this also updates the data rows so they will sync
            tp = addTableFromDefinitionResource(definitionResource);

            Log.w(TAG, "database schema has changed. Structural modifications, if any, were successful.");
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
            if ( msg == null ) msg = e.toString();
            tableResult.setMessage(msg);
            tableResult.setStatus(Status.EXCEPTION);
            return;
          } catch (JsonMappingException e) {
            e.printStackTrace();
            String msg = e.getMessage();
            if ( msg == null ) msg = e.toString();
            tableResult.setMessage(msg);
            tableResult.setStatus(Status.EXCEPTION);
            return;
          } catch (IOException e) {
            e.printStackTrace();
            String msg = e.getMessage();
            if ( msg == null ) msg = e.toString();
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
        }}, pushLocalTableLevelFiles);

        // we found the matching resource on the server and we have set up our
        // local table to be ready for any data merge with the server's table.

        // we should be up-to-date on the schema and properties
        // now fetch all the changed rows...

        // refresh the tp
        tp = TableProperties.refreshTablePropertiesForTable(context, appName, tp.getTableId());

        success = true;
    } catch (IOException e) {
      e.printStackTrace();
      String msg = e.getMessage();
      if ( msg == null ) msg = e.toString();
      tableResult.setMessage(msg);
      tableResult.setStatus(Status.EXCEPTION);
      return;
    } finally {
      endTableTransaction(tp, success);
      // Here we also want to add the TableResult to the value.
      if (success) {
        // Then we should have updated the db and shouldn't have set the
        // TableResult to be exception.
        if (tableResult.getStatus() == Status.EXCEPTION) {
          Log.e(TAG, "tableResult status for table: " + tp.getDbTableName()
              + " was EXCEPTION, and yet success returned true. This shouldn't be possible.");
        } else {
          tableResult.setStatus(Status.SUCCESS);
        }
      }
      mUserResult.addTableResult(tableResult);
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
  public SynchronizationResult synchronizeDataRowsAndAttachments() {
    Log.i(TAG, "entered synchronize()");
    ODKFileUtils.assertDirectoryStructure(appName);

    TableProperties[] tps;
    tps = TableProperties.getTablePropertiesForAll(context, appName);

    // we can assume that all the local table properties should
    // sync with the server.
    for ( TableProperties tp : tps ) {
      // Sync the local media files with the server if the table
      // existed locally before we attempted downloading it.

      synchronizeTableDataRowsAndAttachments(tp);
    }
    return mUserResult;
  }

  /**
   * Synchronize the table data rows.
   * <p>
   * Note that if the db changes under you when calling this method, the tp
   * parameter will become out of date. It should be refreshed after calling
   * this method.
   * <p>
   * This method does NOT synchronize the application files. This means that if
   * any html files downloaded require the {@link TableFileUtils#DIR_FRAMEWORK}
   * directory, for instance, the caller must ensure that the app files are
   * synchronized as well.
   *
   * @param tp
   *          the table to synchronize
   * @param pushLocalTableLevelFiles
   *          true if local table-level files should be pushed up to the server.
   *          e.g. any html files on the device should be pushed to the server
   * @param pushLocalInstanceFiles
   *          if local media files associated with data rows should be pushed
   *          up to the server. The data files on the server are always pulled down.
   */
  private void synchronizeTableDataRowsAndAttachments(TableProperties tp) {
    DbTable table = DbTable.getDbTable(tp);
    // used to get the above from the ACTIVE store. if things go wonky, maybe
    // check to see if it was ACTIVE rather than SERVER for a reason. can't
    // think of one. one thing is that if it fails you'll see a table but won't
    // be able to open it, as there won't be any KVS stuff appropriate for it.
    boolean success = true;
    // Prepare the tableResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    TableResult tableResult = new TableResult(tp.getLocalizedDisplayName(), tp.getTableId());
    beginTableTransaction(tp);
    try {
      // presume success...
      tableResult.setStatus(Status.SUCCESS);
      {
        String tableId = tp.getTableId();
        Log.i(TAG, "REST " + tableId);

        boolean once = true;
        while ( once ) {
          once = false;
          try {

            // confirm that the local schema matches the one on the server.
            TableResource resource = synchronizer.getTableOrNull(tp.getTableId());

            if ( resource == null ) {
              // server does not know about it -- report that
              success = false;
              tableResult.setServerHadSchemaChanges(true);
              tableResult.setMessage("Server no longer has table! Marking it as insert locally. Reset App Server to upload.");
              tableResult.setStatus(Status.TABLE_DOES_NOT_EXIST_ON_SERVER);
              return;
            }

            // test that the schemaETag matches
            // if it doesn't, the user MUST sync app-level files and configuration
            // syncing at the app level will adjust/set the local table properties
            // schemaETag to match that on the server.
            SyncTag syncTag = tp.getSyncTag();
            if ( syncTag == null ||
                 !resource.getSchemaETag().equals(syncTag.getSchemaETag()) ) {
              // schemaETag is not identical
              success = false;
              tableResult.setServerHadSchemaChanges(true);
              tableResult.setMessage("Server schemaETag differs! Sync app-level files and configuration in order to sync this table.");
              tableResult.setStatus(Status.REQUIRE_APP_LEVEL_SYNC);
              return;
            }

            ////////////////////////////////////////////////////
            ////////////////////////////////////////////////////
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // RESTRUCTURE THIS FOR FILE ATTACHMENTS!!!
            // and now sync the data rows...

            IncomingRowModifications modification;
            try {
              modification = synchronizer.getUpdates(tp.getTableId(), tp.getSyncTag());
            } catch (Exception e) {
              String msg = e.getMessage();
              if ( msg == null ) msg = e.toString();
              tableResult.setMessage(msg);
              tableResult.setStatus(Status.EXCEPTION);
              success = false;
              break;
            }

            /**************************
             * PART 2: UPDATE THE DATA
             **************************/
            Log.d(TAG, "updateDbFromServer setServerHadDataChanges(true)");
            tableResult.setServerHadDataChanges(modification.hasTableDataChanged());

            Map<String, SyncRow> changedServerRows = modification.getRows();

            // get all the rows in the data table -- we will iterate through them all.
            UserTable localDataTable = table.rawSqlQuery(
                                      DataTableColumns.SAVEPOINT_TYPE + " IS NOT NULL",
                                                      null, null, null, null, null);

            // these are all the various actions we will need to take:

            // serverRow updated; no matching localRow
            List<FileSyncRow> rowsToInsertLocally = new ArrayList<FileSyncRow>();

            // serverRow updated; localRow SyncState is rest or rest_pending_files
            List<FileSyncRow> rowsToUpdateLocally = new ArrayList<FileSyncRow>();

            // serverRow deleted; localRow SyncState is rest or rest_pending_files
            List<FileSyncRow> rowsToDeleteLocally = new ArrayList<FileSyncRow>();

            // serverRow updated or deleted; localRow SyncState is not rest or rest_pending_files
            List<FileSyncRow> rowsToMoveToConflictingLocally = new ArrayList<FileSyncRow>();

            // localRow SyncState.inserting no changes pulled from server
            List<SyncRow> rowsToInsertOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.updating no changes pulled from server
            List<SyncRow> rowsToUpdateOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.deleting no changes pulled from server
            List<SyncRow> rowsToDeleteOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.rest_pending_files no changes pulled from server
            List<SyncRow> rowsToPushFileAttachments = new ArrayList<SyncRow>();

            // loop through the localRow table
            for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
              Row localRow = localDataTable.getRowAtIndex(i);
              String stateStr = localRow.getDataOrMetadataByElementKey(DataTableColumns.SYNC_STATE);
              SyncState state = SyncState.valueOf(stateStr);

              String rowId = localRow.getRowId();

              // see if there is a change to this row that we need to pull down from the server.
              SyncRow serverRow = changedServerRows.get(rowId);

              if ( serverRow == null ) {
                // the local row wasn't impacted by a server change
                // see if this local row should be pushed to the server.
                if ( state == SyncState.inserting ) {
                  rowsToInsertOnServer.add(convertToSyncRow(tp, localRow));
                } else if ( state == SyncState.updating ) {
                  rowsToUpdateOnServer.add(convertToSyncRow(tp, localRow));
                } else if ( state == SyncState.deleting ) {
                  rowsToDeleteOnServer.add(convertToSyncRow(tp, localRow));
                } else if ( state == SyncState.rest_pending_files ) {
                  rowsToPushFileAttachments.add(convertToSyncRow(tp, localRow));
                }
                // otherwise, it is in the rest state or conflicting state
                // and nothing should be done with it...
                continue;
              }

              // OK -- the server is reporting a change (in serverRow) to the localRow.
              // if the localRow is already in a conflicting state, determine what its
              // ConflictType is. If the localRow holds the earlier server-side change,
              // then skip and look at the next record.
              int localRowConflictTypeBeforeSync = -1;
              if ( state == SyncState.conflicting ) {
                // we need to remove the conflicting records that refer to the prior state of the server
                String localRowConflictTypeBeforeSyncStr = localRow.getDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE);
                localRowConflictTypeBeforeSync = Integer.parseInt(localRowConflictTypeBeforeSyncStr);
                if ( localRowConflictTypeBeforeSync == ConflictType.SERVER_DELETED_OLD_VALUES ||
                     localRowConflictTypeBeforeSync == ConflictType.SERVER_UPDATED_UPDATED_VALUES ) {
                  // This localRow holds the server values from a previously-identified conflict.
                  // Skip it -- we will clean up this copy later once we find the matching localRow
                  // that holds the locally-changed values that were in conflict with this earlier
                  // set of server values.
                  continue;
                }
              }

              // remove this server row from the map of changes reported by the server.
              // the following decision tree will always place the row into one of the
              // local action lists.
              changedServerRows.remove(rowId);

              // OK the record is either a simple local record or a local conflict record
              if (state == SyncState.rest || state == SyncState.rest_pending_files) {
                // the server's change should be applied locally.
                //
                // the file attachments might be stale locally,
                // but those are dealt with separately.

                if (serverRow.isDeleted()) {
                  rowsToDeleteLocally.add(new FileSyncRow(serverRow, convertToSyncRow(tp, localRow), (state == SyncState.rest_pending_files)));
                } else {
                  rowsToUpdateLocally.add(new FileSyncRow(serverRow, convertToSyncRow(tp, localRow), (state == SyncState.rest_pending_files)));
                }
              } else if (serverRow.isDeleted() && (state == SyncState.deleting ||
                  (state == SyncState.conflicting &&
                   localRowConflictTypeBeforeSync == ConflictType.LOCAL_DELETED_OLD_VALUES))) {
                // this occurs if
                // (1) a deleting request was never ACKed but it was performed on the server.
                // (2) if there is an unresolved conflict held locally with the local action
                //     being to delete the record, and the prior server state being a value
                //     change, but the newly sync'd state now reflects a deletion by another
                //     party.
                //

                // no need to worry about server conflict records.
                // any server conflict rows will be deleted during the delete step
                rowsToDeleteLocally.add(new FileSyncRow(serverRow, convertToSyncRow(tp, localRow), false));
              } else {
                // SyncState.deleting  and server is not deleting
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
                } else if(state == SyncState.inserting) {
                  // SyncState.inserting and record exists on server
                  // The 'inserting' case occurs if an insert is never ACKed but
                  // completes successfully on the server.
                  localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
                  Log.i(TAG, "local row was in sync state INSERTING, changing to "
                      + "CONFLICT and setting conflict type to: " + localRowConflictType);
                } else if (state == SyncState.deleting) {
                  // SyncState.deleting  and server is not deleting
                  localRowConflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
                  Log.i(TAG, "local row was in sync state DELETING, changing to "
                      + "CONFLICT and updating conflict type to: " + localRowConflictType);
                } else if (state == SyncState.conflicting) {
                  // SyncState.conflicting and new change on server
                  // leave the local conflict type unchanged (retrieve it and use it).
                  localRowConflictType = localRowConflictTypeBeforeSync;
                  Log.i(TAG, "local row was in sync state CONFLICTING, leaving as "
                      + "CONFLICTING and leaving conflict type unchanged as: "
                      + localRowConflictTypeBeforeSync);
                } else {
                  throw new IllegalStateException("Unexpected state encountered");
                }
                rowsToMoveToConflictingLocally.add(new FileSyncRow(serverRow, convertToSyncRow(tp, localRow), false, localRowConflictType));
              }
            }

            // Now, go through the remaining serverRows in the rows map. That
            // map now contains only row changes that don't affect any existing
            // localRow. If the server change is not a row-deletion / revoke-row action,
            // then insert the serverRow locally.
            for ( SyncRow serverRow : changedServerRows.values() ) {
              boolean isDeleted = serverRow.isDeleted();
              if ( !isDeleted ) {
                rowsToInsertLocally.add(new FileSyncRow(serverRow, null, false));
              }
            }

            //
            // OK we have captured the local inserting, locally updating,
            // locally deleting and conflicting actions.

            // i.e., we have created entries in the various action lists
            // for all the actions we should take.

            /////////////////////////////////////////////////////
            /// PERFORM LOCAL DATABASE CHANGES
            /// PERFORM LOCAL DATABASE CHANGES
            /// PERFORM LOCAL DATABASE CHANGES
            /// PERFORM LOCAL DATABASE CHANGES
            /// PERFORM LOCAL DATABASE CHANGES

            success = deleteRowsInDb(tp, table, rowsToDeleteLocally);
            insertRowsInDb(tp, table, rowsToInsertLocally);
            if ( !updateRowsInDb(tp, table, rowsToUpdateLocally) ) {
              success = false;
            }
            conflictRowsInDb(tp, table, rowsToMoveToConflictingLocally);

            // If we made it here and there was data, then we successfully updated the
            // data from the server.
            if (changedServerRows.size() > 0) {
              tableResult.setPulledServerData(success);
            }

            // TODO: fix this for rest_pending_files
            // We likely need to relax this constraint on the
            // server?

            // We have to set this synctag here so that the server knows we saw its
            // changes. Otherwise it won't let us put up new information.
            if ( success ) {
              tp.setSyncTag(modification.getTableSyncTag());
            }

            ///////////////////////////////////////
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES

            if (rowsToInsertOnServer.size() != 0 || rowsToUpdateOnServer.size() != 0 || rowsToDeleteOnServer.size() != 0) {
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
              for (SyncRow syncRow : rowsToInsertOnServer) {
                RowModification rm = synchronizer.insertOrUpdateRow(tableId, revisedTag, syncRow);

                ContentValues values = new ContentValues();
                values.put(DataTableColumns.ROW_ETAG, rm.getRowETag());
                values.put(DataTableColumns.SYNC_STATE, SyncState.rest_pending_files.name());
                table.actualUpdateRowByRowId(rm.getRowId(), values);
                syncResult.stats.numInserts++;
                syncResult.stats.numEntries++;

                boolean outcome = synchronizer.putFileAttachments(tp.getTableId(), syncRow);
                if ( outcome ) {
                  // move to rest state
                  values.clear();
                  values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
                  table.actualUpdateRowByRowId(syncRow.getRowId(), values);
                }

                revisedTag = rm.getTableSyncTag();
                if ( success ) {
                  tp.setSyncTag(revisedTag);
                }
              }
              for (SyncRow syncRow : rowsToUpdateOnServer) {
                RowModification rm = synchronizer.insertOrUpdateRow(tableId, revisedTag, syncRow);

                ContentValues values = new ContentValues();
                values.put(DataTableColumns.ROW_ETAG, rm.getRowETag());
                values.put(DataTableColumns.SYNC_STATE, SyncState.rest_pending_files.name());
                table.actualUpdateRowByRowId(rm.getRowId(), values);
                syncResult.stats.numUpdates++;
                syncResult.stats.numEntries++;

                boolean outcome = synchronizer.putFileAttachments(tp.getTableId(), syncRow);
                if ( outcome ) {
                  // move to rest state
                  values.clear();
                  values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
                  table.actualUpdateRowByRowId(syncRow.getRowId(), values);
                }

                revisedTag = rm.getTableSyncTag();
                if ( success ) {
                  tp.setSyncTag(revisedTag);
                }
              }
              for (SyncRow syncRow : rowsToDeleteOnServer) {
                RowModification rm = synchronizer.deleteRow(tableId, revisedTag, syncRow);
                table.deleteRowActual(rm.getRowId());
                syncResult.stats.numDeletes++;
                syncResult.stats.numEntries++;
                revisedTag = rm.getTableSyncTag();
                if ( success ) {
                  tp.setSyncTag(revisedTag);
                }
              }

              // And try to push the file attachments...
              for (SyncRow syncRow : rowsToPushFileAttachments) {
                boolean outcome = synchronizer.putFileAttachments(tableId, syncRow);
                if ( outcome ) {
                  outcome = synchronizer.getFileAttachments(tableId, syncRow, true);
                  if ( outcome ) {
                    ContentValues values = new ContentValues();
                    values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
                    table.actualUpdateRowByRowId(syncRow.getRowId(), values);
                  }
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
            ////////////////////////////////////////////////////////////
            ////////////////////////////////////////////////////////////

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
      if (success && tp != null) // null in case we deleted the tp.
        tp.setLastSyncTime(du.formatNowForDb());
    } finally {
      endTableTransaction(tp, success);
      // Here we also want to add the TableResult to the value.
      if (success) {
        // Then we should have updated the db and shouldn't have set the
        // TableResult to be exception.
        if (tableResult.getStatus() == Status.EXCEPTION) {
          Log.e(TAG, "tableResult status for table: " + tp.getDbTableName()
              + " was EXCEPTION, and yet success returned true. This shouldn't be possible.");
        } else {
          tableResult.setStatus(Status.SUCCESS);
        }
      }
      mUserResult.addTableResult(tableResult);
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

    FileSyncRow(SyncRow serverRow, SyncRow localRow, boolean isRestPendingFiles, int localRowConflictType) {
      this.serverRow = serverRow;
      this.localRow = localRow;
      this.isRestPendingFiles = isRestPendingFiles;
      this.localRowConflictType = localRowConflictType;
    }
  };

  private void resourceAccessException(String method, TableProperties tp,
                                       ResourceAccessException e, TableResult tableResult) {
    Log.e(TAG,
        String.format("ResourceAccessException in %s for table: %s", method, tp.getTableId()),
        e);
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(e.getMessage());
    syncResult.stats.numIoExceptions++;
  }

  private void
      ioException(String method, TableProperties tp, IOException e, TableResult tableResult) {
    Log.e(TAG, String.format("IOException in %s for table: %s", method, tp.getTableId()), e);
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(e.getMessage());
    syncResult.stats.numIoExceptions++;
  }

  private void exception(String method, TableProperties tp, Exception e, TableResult tableResult) {
    Log.e(TAG,
        String.format("Unexpected exception in %s on table: %s", method, tp.getTableId()), e);
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(e.getMessage());
  }

  private void conflictRowsInDb(TableProperties tp, DbTable table, List<FileSyncRow> changes) throws IOException {

    for (FileSyncRow change : changes) {
      SyncRow serverRow = change.serverRow;
      Log.i(TAG, "conflicting row, id=" + serverRow.getRowId() + " rowETag=" + serverRow.getRowETag());
      ContentValues values = new ContentValues();

      // delete the old server-values conflicting row if it exists
      String whereClause = String.format("%s = ? AND %s = ? AND %s IN " + "( ?, ? )",
          DataTableColumns.ID, DataTableColumns.SYNC_STATE, DataTableColumns.CONFLICT_TYPE);
      String[] whereArgs = { serverRow.getRowId(), SyncState.conflicting.name(),
          String.valueOf(ConflictType.SERVER_DELETED_OLD_VALUES),
          String.valueOf(ConflictType.SERVER_UPDATED_UPDATED_VALUES)
      };
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

      if ( serverRowConflictType == ConflictType.SERVER_DELETED_OLD_VALUES &&
           localRowConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES ) {

        // special case -- the server and local rows are both being deleted
        // just delete them!
        table.deleteRowActual(serverRow.getRowId());
        syncResult.stats.numDeletes++;
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
        values.put(DataTableColumns.FILTER_TYPE, (type == null) ? Scope.Type.DEFAULT.name() : type.name());
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

        syncResult.stats.numConflictDetectedExceptions++;
        syncResult.stats.numEntries += 2;

        // ensure we have the file attachments for the conflicting row
        // it is OK if we can't get them, but they may be useful for reconciliation
        boolean outcome = synchronizer.getFileAttachments(tp.getTableId(), serverRow, false);
        if ( !outcome ) {
          // we don't do anything on failure -- just log a warning.
          // we need to leave the sync state as conflicting.
          Log.w(TAG,"Unable to fetch file attachments from conflicting row on server");
        }
      }
    }
  }

  private void insertRowsInDb(TableProperties tp, DbTable table, List<FileSyncRow> changes) {
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
      syncResult.stats.numInserts++;
      syncResult.stats.numEntries++;

      // ensure we have the file attachments for the inserted row
      boolean outcome = synchronizer.getFileAttachments(tp.getTableId(), serverRow, true);
      if ( outcome ) {
        // move to rest state
        values.clear();
        values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
        table.actualUpdateRowByRowId(serverRow.getRowId(), values);
      }
    }
  }

  private boolean updateRowsInDb(TableProperties tp, DbTable table, List<FileSyncRow> changes) {
    boolean success = true;

    for (FileSyncRow change : changes) {
      // if the localRow sync state was rest_pending_files,
      // ensure that all those files are uploaded before
      // we update the row. This ensures that all attachments
      // are saved before we revise the local row value.
      boolean outcome = true;
      if ( change.isRestPendingFiles ) {
        // we need to push our changes to the server first...
        outcome = synchronizer.putFileAttachments(tp.getTableId(), change.localRow);
      }

      if ( !outcome ) {
        // leave this row stale because we haven't been able to
        // finish the post of the older row's file.
        success = false;
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
        syncResult.stats.numUpdates++;
        syncResult.stats.numEntries++;

        // and try to get the file attachments for the row
        outcome = synchronizer.getFileAttachments(tp.getTableId(), serverRow, true);
        if ( outcome ) {
          // move to rest state
          values.clear();
          values.put(DataTableColumns.SYNC_STATE, SyncState.rest.name());
          table.actualUpdateRowByRowId(serverRow.getRowId(), values);
        }
        // otherwise, leave in rest_pending_files state.
      }
    }
    return success;
  }

  private boolean deleteRowsInDb(TableProperties tp, DbTable table, List<FileSyncRow> changes) throws IOException {
    boolean deletesAllSuccessful = true;
    for ( FileSyncRow change : changes ) {
      if ( change.isRestPendingFiles ) {
        boolean outcome = synchronizer.putFileAttachments(tp.getTableId(), change.localRow);
        if ( outcome ) {
          table.deleteRowActual(change.serverRow.getRowId());
          syncResult.stats.numDeletes++;
        } else {
          deletesAllSuccessful = false;
        }
      }
    }
    return deletesAllSuccessful;
  }

  private SyncRow convertToSyncRow(TableProperties tp, Row localRow) {
    String rowId = localRow.getRowId();
    String rowETag = localRow.getDataOrMetadataByElementKey(DataTableColumns.ROW_ETAG);
    Map<String, String> values = new HashMap<String, String>();

    for ( ColumnProperties cp : tp.getAllColumns().values() ) {
      if ( cp.isUnitOfRetention() ) {
        String elementKey = cp.getElementKey();
        values.put(elementKey, localRow.getDataOrMetadataByElementKey(elementKey));
      }
    }
    SyncRow syncRow = new SyncRow(
                              rowId,
                              rowETag,
                              false,
                              localRow.getDataOrMetadataByElementKey(DataTableColumns.FORM_ID),
                              localRow.getDataOrMetadataByElementKey(DataTableColumns.LOCALE),
                              localRow.getDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TYPE),
                              localRow.getDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TIMESTAMP),
                              localRow.getDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_CREATOR),
                              Scope.asScope(localRow.getDataOrMetadataByElementKey(DataTableColumns.FILTER_TYPE),
                                  localRow.getDataOrMetadataByElementKey(DataTableColumns.FILTER_VALUE)),
                              values);
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
        TableDefinitionResource definitionResource) throws JsonParseException,
      JsonMappingException, IOException, SchemaMismatchException {
    TableProperties tp = TableProperties.refreshTablePropertiesForTable(context, appName,
        definitionResource.getTableId());
    if (tp == null) {
      tp = TableProperties
          .addTable(context, appName, definitionResource.getTableId(), definitionResource.getTableId(),
              definitionResource.getTableId());
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
          if ( cpListChildElementKeys == null ) {
            cpListChildElementKeys = new ArrayList<String>();
          }
          if (!(
              (cp.getElementName() == col.getElementName() ||
               ((cp.getElementName() != null) && cp.getElementName().equals(col.getElementName())) )
            && cp.isUnitOfRetention() == DataHelper.intToBool(col.getIsUnitOfRetention())
            && cpListChildElementKeys.size() == listChildElementKeys.size()
            && cpListChildElementKeys.containsAll(listChildElementKeys) )) {
            throw new SchemaMismatchException("Server schema differs from local schema");
          } else if (!cp.getColumnType().equals(ColumnType.valueOf(col.getElementType()))) {
            // we have a column datatype change.
            // we should be able to handle this for simple types (unknown -> text
            // or text -> integer)
            throw new SchemaMismatchException("Server schema differs from local schema (column datatype change)");
          }
        }
      }

      SyncTag syncTag = tp.getSyncTag();
      if ( syncTag == null ||
          !syncTag.getSchemaETag().equals(definitionResource.getSchemaETag()) ) {
        // server has changed its schema
        // this means that the server may have none of our local data
        // or may not have any of the original server conflict rows.
        // Clean up this table and set the dataETag to null.
        DbTable table = DbTable.getDbTable(tp);
        table.changeDataRowsToInsertingState();
        // and update to the new schemaETag, but clear our dataETag
        // so that all data rows sync.
        tp.setSyncTag(new SyncTag(null, definitionResource.getSchemaETag()));
      }
    }
    tp.setIsSetToSync(true);
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
