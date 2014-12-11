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
import java.util.Map;
import java.util.Set;

import org.apache.wink.client.ClientWebException;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.ElementDataType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.ChangeSetList;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome.OutcomeType;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.RowResource;
import org.opendatakit.aggregate.odktables.rest.entity.RowResourceList;
import org.opendatakit.aggregate.odktables.rest.entity.Scope;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.UserTable.Row;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;
import org.opendatakit.common.android.utilities.TableUtil;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.sync.SynchronizationResult.Status;
import org.opendatakit.sync.service.SyncProgressState;

import android.content.ContentValues;
import android.database.sqlite.SQLiteDatabase;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * SyncProcessor implements the cloud synchronization logic for Tables.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class ProcessRowDataChanges {

  private static final String TAG = ProcessRowDataChanges.class.getSimpleName();

  private static final int UPSERT_BATCH_SIZE = 500;
  private static final int ROWS_BETWEEN_PROGRESS_UPDATES = 10;
  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
  }

  private WebLogger log;

  private final SyncExecutionContext sc;

  private Double perRowIncrement;
  private int rowsProcessed;

  public ProcessRowDataChanges(SyncExecutionContext sharedContext) {
    this.sc = sharedContext;
    this.log = WebLogger.getLogger(sc.getAppName());
  }

  /**
   * Common error reporting...
   * 
   * @param method
   * @param tableId
   * @param e
   * @param tableResult
   */
  private void clientWebException(String method, String tableId, ClientWebException e,
      TableResult tableResult) {
    log.e(TAG, String.format("ResourceAccessException in %s for table: %s exception: %s", method,
        tableId, e.toString()));
    tableResult.setStatus(Status.AUTH_EXCEPTION);
    tableResult.setMessage(e.getMessage());
  }

  /**
   * Common error reporting...
   * 
   * @param method
   * @param tableId
   * @param e
   * @param tableResult
   */
  private void exception(String method, String tableId, Exception e, TableResult tableResult) {
    log.e(
        TAG,
        String.format("Unexpected exception in %s on table: %s exception: %s", method, tableId,
            e.toString()));
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(e.getMessage());
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
   * 
   * @param workingListOfTables
   *          -- the list of tables we should sync with the server. This will be
   *          a subset of the available local tables -- if there was any error
   *          during the sync'ing of the table-level files, or if the table
   *          schema does not match, the local table will be omitted from this
   *          list.
   */
  public void synchronizeDataRowsAndAttachments(List<TableResource> workingListOfTables,
      boolean deferInstanceAttachments) {
    log.i(TAG, "entered synchronize()");

    SQLiteDatabase db = null;

    // we can assume that all the local table properties should
    // sync with the server.
    for (TableResource tableResource : workingListOfTables) {
      // Sync the local media files with the server if the table
      // existed locally before we attempted downloading it.

      String tableId = tableResource.getTableId();
      TableDefinitionEntry te;
      ArrayList<ColumnDefinition> orderedDefns;
      String displayName;
      try {
        db = sc.getDatabase();
        te = ODKDatabaseUtils.get().getTableDefinitionEntry(db, tableId);
        orderedDefns = TableUtil.get().getColumnDefinitions(db, sc.getAppName(), tableId);
        displayName = TableUtil.get().getLocalizedDisplayName(db, tableId);
      } finally {
        if (db != null) {
          db.close();
          db = null;
        }
      }

      synchronizeTableDataRowsAndAttachments(tableResource, te, orderedDefns, displayName,
          deferInstanceAttachments);
      sc.incMajorSyncStep();
    }
  }

  private UserTable updateLocalRowsFromServerChanges(TableResource tableResource,
      TableDefinitionEntry te, ArrayList<ColumnDefinition> orderedColumns, String displayName,
      boolean deferInstanceAttachments, ArrayList<ColumnDefinition> fileAttachmentColumns, 
      List<SyncRowPending> rowsToPushFileAttachments, UserTable localDataTable, RowResourceList rows) throws IOException {
    
    String tableId = tableResource.getTableId();
    TableResult tableResult = sc.getTableResult(tableId);
    
    if ( rows.getRows().isEmpty() ) {
      // nothing here -- let caller determine whether we are done or 
      // whether we need to issue another request to the server.
      return localDataTable;
    }
    
    Map<String, SyncRow> changedServerRows = new HashMap<String, SyncRow>();
    for (RowResource row : rows.getRows()) {
      SyncRow syncRow = new SyncRow(row.getRowId(), row.getRowETag(), row.isDeleted(),
          row.getFormId(), row.getLocale(), row.getSavepointType(), row.getSavepointTimestamp(),
          row.getSavepointCreator(), row.getFilterScope(), row.getValues(), fileAttachmentColumns);
      changedServerRows.put(row.getRowId(), syncRow);
    }

    sc.updateNotification(SyncProgressState.ROWS, R.string.anaylzing_row_changes,
        new Object[] { tableId }, 7.0, false);

    /**************************
     * PART 2: UPDATE THE DATA
     **************************/
    log.d(TAG, "updateDbFromServer setServerHadDataChanges(true)");
    tableResult.setServerHadDataChanges(!changedServerRows.isEmpty());
    // these are all the various actions we will need to take:

    // serverRow updated; no matching localRow
    List<SyncRowDataChanges> rowsToInsertLocally = new ArrayList<SyncRowDataChanges>();

    // serverRow updated; localRow SyncState is synced or
    // synced_pending_files
    List<SyncRowDataChanges> rowsToUpdateLocally = new ArrayList<SyncRowDataChanges>();

    // serverRow deleted; localRow SyncState is synced or
    // synced_pending_files
    List<SyncRowDataChanges> rowsToDeleteLocally = new ArrayList<SyncRowDataChanges>();

    // serverRow updated or deleted; localRow SyncState is not synced or
    // synced_pending_files
    List<SyncRowDataChanges> rowsToMoveToInConflictLocally = new ArrayList<SyncRowDataChanges>();

    // loop through the localRow table
    for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
      Row localRow = localDataTable.getRowAtIndex(i);
      String stateStr = localRow
          .getRawDataOrMetadataByElementKey(DataTableColumns.SYNC_STATE);
      SyncState state = stateStr == null ? null : SyncState.valueOf(stateStr);

      String rowId = localRow.getRowId();

      // see if there is a change to this row from our current
      // server change set.
      SyncRow serverRow = changedServerRows.get(rowId);

      if (serverRow == null) {
        continue;
      }

      // OK -- the server is reporting a change (in serverRow) to the
      // localRow.
      // if the localRow is already in a in_conflict state, determine
      // what its
      // ConflictType is. If the localRow holds the earlier server-side
      // change,
      // then skip and look at the next record.
      int localRowConflictTypeBeforeSync = -1;
      if (state == SyncState.in_conflict) {
        // we need to remove the in_conflict records that refer to the
        // prior state of the server
        String localRowConflictTypeBeforeSyncStr = localRow
            .getRawDataOrMetadataByElementKey(DataTableColumns.CONFLICT_TYPE);
        localRowConflictTypeBeforeSync = localRowConflictTypeBeforeSyncStr == null ? null
            : Integer.parseInt(localRowConflictTypeBeforeSyncStr);
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
      // in_conflict record
      if (state == SyncState.synced || state == SyncState.synced_pending_files) {
        // the server's change should be applied locally.
        //
        // the file attachments might be stale locally,
        // but those are dealt with separately.

        if (serverRow.isDeleted()) {
          rowsToDeleteLocally.add(new SyncRowDataChanges(serverRow, 
              SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns, localRow),
              (state == SyncState.synced_pending_files)));
        } else {
          rowsToUpdateLocally.add(new SyncRowDataChanges(serverRow, 
              SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns, localRow),
              (state == SyncState.synced_pending_files)));
        }
      } else if (serverRow.isDeleted()
          && (state == SyncState.deleted || (state == SyncState.in_conflict && localRowConflictTypeBeforeSync == ConflictType.LOCAL_DELETED_OLD_VALUES))) {
        // this occurs if
        // (1) a delete request was never ACKed but it was performed
        // on the server.
        // (2) if there is an unresolved conflict held locally with the
        // local action being to delete the record, and the prior server
        // state being a value change, but the newly sync'd state now
        // reflects a deletion by another party.
        //

        // no need to worry about server in_conflict records.
        // any server in_conflict rows will be deleted during the delete
        // step
        rowsToDeleteLocally.add(new SyncRowDataChanges(serverRow, 
            SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns, localRow),
            false));
      } else {
        // SyncState.deleted and server is not deleting
        // SyncState.new_row and record exists on server
        // SyncState.changed and new change on server
        // SyncState.in_conflict and new change on server

        // no need to worry about server in_conflict records.
        // any server in_conflict rows will be cleaned up during the
        // update of the in_conflict state.

        // figure out what the localRow conflict type should be...
        Integer localRowConflictType;
        if (state == SyncState.changed) {
          // SyncState.changed and new change on server
          localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
          log.i(TAG, "local row was in sync state CHANGED, changing to "
              + "IN_CONFLICT and setting conflict type to: " + localRowConflictType);
        } else if (state == SyncState.new_row) {
          // SyncState.new_row and record exists on server
          // The 'new_row' case occurs if an insert is never ACKed but
          // completes successfully on the server.
          localRowConflictType = ConflictType.LOCAL_UPDATED_UPDATED_VALUES;
          log.i(TAG, "local row was in sync state NEW_ROW, changing to "
              + "IN_CONFLICT and setting conflict type to: " + localRowConflictType);
        } else if (state == SyncState.deleted) {
          // SyncState.deleted and server is not deleting
          localRowConflictType = ConflictType.LOCAL_DELETED_OLD_VALUES;
          log.i(TAG, "local row was in sync state DELETED, changing to "
              + "IN_CONFLICT and updating conflict type to: " + localRowConflictType);
        } else if (state == SyncState.in_conflict) {
          // SyncState.in_conflict and new change on server
          // leave the local conflict type unchanged (retrieve it and
          // use it).
          localRowConflictType = localRowConflictTypeBeforeSync;
          log.i(TAG, "local row was in sync state IN_CONFLICT, leaving as "
              + "IN_CONFLICT and leaving conflict type unchanged as: "
              + localRowConflictTypeBeforeSync);
        } else {
          throw new IllegalStateException("Unexpected state encountered");
        }
        SyncRowDataChanges syncRow = new SyncRowDataChanges(serverRow, 
            SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns, localRow),
            false, localRowConflictType);

        if (!syncRow.identicalValues(orderedColumns)) {
          if (syncRow.identicalValuesExceptRowETagAndFilterScope(orderedColumns)) {
            // just apply the server RowETag and filterScope to the
            // local row
            rowsToUpdateLocally.add(new SyncRowDataChanges(serverRow, 
                SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns, localRow),
                true));
          } else {
            rowsToMoveToInConflictLocally.add(syncRow);
          }
        } else {
          log.w(TAG, "identical rows returned from server -- SHOULDN'T THESE NOT HAPPEN?");
        }
      }
    }

    // Now, go through the remaining serverRows in the rows map. That
    // map now contains only row changes that don't affect any existing
    // localRow. If the server change is not a row-deletion / revoke-row
    // action, then insert the serverRow locally.
    for (SyncRow serverRow : changedServerRows.values()) {
      boolean isDeleted = serverRow.isDeleted();
      if (!isDeleted) {
        rowsToInsertLocally.add(new SyncRowDataChanges(serverRow, null, false));
      }
    }

    //
    // OK we have captured the local inserting, locally updating,
    // locally deleting and conflicting actions. And we know
    // the changes for the server. Determine the per-row percentage
    // for applying all these changes

    int totalChange = rowsToInsertLocally.size() + rowsToUpdateLocally.size()
        + rowsToDeleteLocally.size() + rowsToMoveToInConflictLocally.size();

    perRowIncrement = 70.0 / ((double) (totalChange + 1));
    rowsProcessed = 0;
    boolean hasAttachments = !fileAttachmentColumns.isEmpty();

    // i.e., we have created entries in the various action lists
    // for all the actions we should take.

    // ///////////////////////////////////////////////////
    // / PERFORM LOCAL DATABASE CHANGES
    // / PERFORM LOCAL DATABASE CHANGES
    // / PERFORM LOCAL DATABASE CHANGES
    // / PERFORM LOCAL DATABASE CHANGES
    // / PERFORM LOCAL DATABASE CHANGES

    {
      SQLiteDatabase db = null;
      try {
        db = sc.getDatabase();

        // this will individually move some files to the locally-deleted state
        // if we cannot sync file attachments in those rows.
        pushLocalAttachmentsBeforeDeleteRowsInDb(db, tableResource,
            rowsToDeleteLocally, fileAttachmentColumns, deferInstanceAttachments,
            tableResult);

        // and now do a big transaction to update the local database.
        db.beginTransaction();

        deleteRowsInDb(db, tableResource, rowsToDeleteLocally, fileAttachmentColumns,
            deferInstanceAttachments, tableResult);

        insertRowsInDb(db, tableResource, orderedColumns, rowsToInsertLocally,
            rowsToPushFileAttachments, hasAttachments, tableResult);

        updateRowsInDb(db, tableResource, orderedColumns, rowsToUpdateLocally,
            rowsToPushFileAttachments, hasAttachments, tableResult);

        conflictRowsInDb(db, tableResource, orderedColumns, rowsToMoveToInConflictLocally,
            rowsToPushFileAttachments, hasAttachments, tableResult);

        localDataTable = ODKDatabaseUtils.get().rawSqlQuery(db, sc.getAppName(), tableId,
            orderedColumns, null, null, null, null, DataTableColumns.ID, "ASC");

        // TODO: fix this for synced_pending_files
        // We likely need to relax this constraint on the
        // server?

        db.setTransactionSuccessful();
      } finally {
        if (db != null) {
          db.endTransaction();
          db.close();
          db = null;
        }
      }
    }
    
    return localDataTable;
  }

  /**
   * Synchronize the table data rows.
   * <p>
   * Note that if the db changes under you when calling this method, the tp
   * parameter will become out of date. It should be refreshed after calling
   * this method.
   * <p>
   * This method does NOT synchronize any non-instance files; it assumes the
   * database schema has already been sync'd.
   *
   * @param tableResource
   *          the table resource from the server, either from the getTables()
   *          call or from a createTable() response.
   * @param te
   *          definition of the table to synchronize
   * @param orderedColumns
   *          well-formed ordered list of columns in this table.
   * @param displayName
   *          display name for this tableId - used in notifications
   * @param deferInstanceAttachments
   *          true if new instance attachments should NOT be pulled from or
   *          pushed to the server. e.g., for bandwidth management.
   */
  private void synchronizeTableDataRowsAndAttachments(TableResource tableResource,
      TableDefinitionEntry te, ArrayList<ColumnDefinition> orderedColumns, String displayName,
      boolean deferInstanceAttachments) {
    boolean success = true;
    boolean instanceFileSuccess = true;

    ArrayList<ColumnDefinition> fileAttachmentColumns = new ArrayList<ColumnDefinition>();
    for (ColumnDefinition cd : orderedColumns) {
      if (cd.getType().getDataType() == ElementDataType.rowpath) {
        fileAttachmentColumns.add(cd);
      }
    }

    log.i(
        TAG,
        "synchronizeTableDataRowsAndAttachments - deferInstanceAttachments: "
            + Boolean.toString(deferInstanceAttachments));

    // Prepare the tableResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    String tableId = te.getTableId();
    TableResult tableResult = sc.getTableResult(tableId);
    tableResult.setTableDisplayName(displayName);
    if (tableResult.getStatus() != Status.WORKING) {
      // there was some sort of error...
      log.e(TAG, "Skipping data sync - error in table schema or file verification step " + tableId);
      return;
    }

    if (tableId.equals("framework")) {
      // do not sync the framework table
      tableResult.setStatus(Status.SUCCESS);
      sc.updateNotification(SyncProgressState.ROWS, R.string.table_data_sync_complete,
          new Object[] { tableId }, 100.0, false);
      return;
    }

    boolean containsConflicts = false;

    try {
      {
        log.i(TAG, "REST " + tableId);

        boolean once = true;
        while (once) {
          once = false;
          try {

            sc.updateNotification(SyncProgressState.ROWS,
                R.string.verifying_table_schema_on_server, new Object[] { tableId }, 0.0, false);

            // test that the schemaETag matches
            // if it doesn't, the user MUST sync app-level files and
            // configuration
            // syncing at the app level will adjust/set the local table
            // properties
            // schemaETag to match that on the server.
            String schemaETag = te.getSchemaETag();
            if (schemaETag == null || !tableResource.getSchemaETag().equals(schemaETag)) {
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
            // get all the rows in the data table -- we will iterate through
            // them all.
            UserTable localDataTable;
            {
              SQLiteDatabase db = null;

              try {
                db = sc.getDatabase();
                localDataTable = ODKDatabaseUtils.get().rawSqlQuery(db, sc.getAppName(), tableId,
                    orderedColumns, null, null, null, null, DataTableColumns.ID, "ASC");
              } finally {
                if (db != null) {
                  db.close();
                  db = null;
                }
              }
            }
            
            // server
            List<SyncRowPending> rowsToPushFileAttachments = new ArrayList<SyncRowPending>();

            containsConflicts = localDataTable.hasConflictRows();

            if (localDataTable.hasCheckpointRows()) {
              tableResult.setMessage(sc.getString(R.string.table_contains_checkpoints));
              tableResult.setStatus(Status.TABLE_CONTAINS_CHECKPOINTS);
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

            sc.updateNotification(SyncProgressState.ROWS,
                R.string.getting_changed_rows_on_server, new Object[] { tableId }, 5.0, false);

            // TODO: REDO
            // TODO: REDO
            // TODO: REDO
            // TODO: REDO
            // TODO: REDO
            // TODO: REDO
            // TODO: REDO
            // TODO: REDO
            // TODO: REDO
            // TODO: REDO
            // this needs to be re-worked since we can have continuation cursors in play...
            String firstDataETag = null;
            String websafeResumeCursor = null;
            for (;;) {
              RowResourceList rows = null;
              
              try {
                rows = sc.getSynchronizer().getUpdates(tableResource, te.getLastDataETag(), websafeResumeCursor);
                if ( firstDataETag == null ) {
                  firstDataETag = rows.getDataETag();
                }
              } catch (Exception e) {
                String msg = e.getMessage();
                if (msg == null) {
                  msg = e.toString();
                }
                success = false;
                tableResult.setMessage(msg);
                tableResult.setStatus(Status.EXCEPTION);
                return;
              }
  
              localDataTable = updateLocalRowsFromServerChanges(tableResource, te, orderedColumns, displayName, 
                  deferInstanceAttachments, fileAttachmentColumns, rowsToPushFileAttachments, localDataTable, rows);
              
              if ( rows.isHasMoreResults() ) {
                websafeResumeCursor = rows.getWebSafeResumeCursor();
              } else {
                break;
              }
            }

            // If we made it here and there was data, then we successfully
            // updated the localDataTable from the server.
            tableResult.setPulledServerData(success);

            
            // We have to set the syncTag here so that the server
            // knows we saw its changes. Otherwise it won't let us
            // put up new information.
            if (success) {
              SQLiteDatabase db = null;

              try {
                db = sc.getDatabase();
                // update the dataETag to the one returned by the first
                // of the fetch queries, above.
                ODKDatabaseUtils.get().updateDBTableETags(db, tableId,
                    tableResource.getSchemaETag(), firstDataETag);
                // and be sure to update our in-memory objects...
                te.setSchemaETag(tableResource.getSchemaETag());
                te.setLastDataETag(firstDataETag);
                tableResource.setDataETag(firstDataETag);
              } finally {
                if (db != null) {
                  db.close();
                  db = null;
                }
              }
            }

            // OK. We can now scan through the localDataTable for changes that 
            // should be sent up to the server.
            
            sc.updateNotification(SyncProgressState.ROWS, R.string.anaylzing_row_changes,
                new Object[] { tableId }, 70.0, false);

            /**************************
             * PART 2: UPDATE THE DATA
             **************************/
            // these are all the various actions we will need to take:

            // localRow SyncState.new_row no changes pulled from server
            // localRow SyncState.changed no changes pulled from server
            // localRow SyncState.deleted no changes pulled from server
            List<SyncRow> allAlteredRows = new ArrayList<SyncRow>();

            // loop through the localRow table
            for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
              Row localRow = localDataTable.getRowAtIndex(i);
              String stateStr = localRow
                  .getRawDataOrMetadataByElementKey(DataTableColumns.SYNC_STATE);
              SyncState state = stateStr == null ? null : SyncState.valueOf(stateStr);

              String rowId = localRow.getRowId();

              // the local row wasn't impacted by a server change
              // see if this local row should be pushed to the server.
              if (state == SyncState.new_row || 
                  state == SyncState.changed || 
                  state == SyncState.deleted) {
                allAlteredRows.add(
                    SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns, localRow));
              } else if (state == SyncState.synced_pending_files) {
                rowsToPushFileAttachments.add(new SyncRowPending(
                    SyncRow.convertToSyncRow(orderedColumns, fileAttachmentColumns, localRow),
                    false, true, true));
              }
            }

            // We know the changes for the server. Determine the per-row percentage
            // for applying all these changes

            int totalChange = allAlteredRows.size() + rowsToPushFileAttachments.size();

            perRowIncrement = 90.0 / ((double) (totalChange + 1));
            rowsProcessed = 0;
            boolean hasAttachments = !fileAttachmentColumns.isEmpty();

            // i.e., we have created entries in the various action lists
            // for all the actions we should take.

            // /////////////////////////////////////
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES
            // SERVER CHANGES

            if (allAlteredRows.size() != 0) {
              if (tableResult.hadLocalDataChanges()) {
                log.e(TAG, "synchronizeTableSynced hadLocalDataChanges() returned "
                    + "true, and we're about to set it to true again. Odd.");
              }
              tableResult.setHadLocalDataChanges(true);
            }

            Set<String> dataETagsFromAlterations = new HashSet<String>();
            
            // push the changes up to the server
            boolean serverSuccess = false;
            try {

              // idempotent interface means that the interactions
              // for inserts, updates and deletes are identical.
              int count = 0;

              ArrayList<RowOutcome> specialCases = new ArrayList<RowOutcome>();

              if (!allAlteredRows.isEmpty()) {
                int offset = 0;
                while (offset < allAlteredRows.size()) {
                  // alter UPSERT_BATCH_SIZE rows at a time to the server
                  int max = offset + UPSERT_BATCH_SIZE;
                  if (max > allAlteredRows.size()) {
                    max = allAlteredRows.size();
                  }
                  List<SyncRow> segmentAlter = allAlteredRows.subList(offset, max);
                  RowOutcomeList outcomes = sc.getSynchronizer().alterRows(tableResource, segmentAlter);

                  if (outcomes.getRows().size() != segmentAlter.size()) {
                    throw new IllegalStateException("Unexpected partial return?");
                  }

                  // process outcomes...
                  count = processRowOutcomes(te, tableResource, tableResult, orderedColumns,
                      fileAttachmentColumns, hasAttachments, rowsToPushFileAttachments, count,
                      allAlteredRows.size(), segmentAlter, outcomes.getRows(), specialCases);

                  // NOTE: specialCases should probably be deleted?
                  // This is the case if the user doesn't have permissions...
                  // TODO: figure out whether these are benign or need reporting....
                  if ( !specialCases.isEmpty() ) {
                    throw new IllegalStateException("update request rejected by the server -- do you have table synchronize privileges?");
                  }

                  // record the dataETag of the update.
                  dataETagsFromAlterations.add(outcomes.getDataETag());
                  
                  // process next segment...
                  offset = max;
                }
              }

              // And now update that we've pushed our changes to the server.
              tableResult.setPushedLocalData(true);

              // specialCases is always empty...
              serverSuccess = true;

              // And try to push the file attachments...
              count = 0;
              for (SyncRowPending syncRowPending : rowsToPushFileAttachments) {
                boolean outcome = true;
                if (!syncRowPending.onlyGetFiles()) {
                  outcome = sc.getSynchronizer().putFileAttachments(tableResource.getInstanceFilesUri(),
                      tableId, syncRowPending, deferInstanceAttachments);
                }
                if (outcome) {
                  outcome = sc.getSynchronizer().getFileAttachments(tableResource.getInstanceFilesUri(),
                      tableId, syncRowPending, deferInstanceAttachments);

                  if (syncRowPending.updateSyncState()) {
                    if (outcome) {
                      // OK -- we succeeded in putting/getting all attachments
                      // update our state to the synced state.
                      SQLiteDatabase db = null;

                      try {
                        db = sc.getDatabase();
                        ODKDatabaseUtils.get().updateRowETagAndSyncState(db, tableId,
                            syncRowPending.getRowId(), syncRowPending.getRowETag(),
                            SyncState.synced);
                      } finally {
                        if (db != null) {
                          db.close();
                          db = null;
                        }
                      }
                    } else {
                      // only care about instance file status if we are trying
                      // to update state
                      instanceFileSuccess = false;
                    }
                  }
                }
                tableResult.incLocalAttachmentRetries();
                ++count;
                ++rowsProcessed;
                if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
                  sc.updateNotification(SyncProgressState.ROWS,
                      R.string.uploading_attachments_server_row, new Object[] { tableId, count,
                          rowsToPushFileAttachments.size() }, 10.0 + rowsProcessed
                          * perRowIncrement, false);
                }
              }

              // OK. Now we have pushed everything.
              rowsToPushFileAttachments.clear();
              
              // We need to find all the changeSets since the firstDataETag and remove the 
              // dataETagsFromAlterations from that set (those are the changes we made), then 
              // fetch all of the active records for the remaining changeSets, and apply them 
              // locally.
              //
              // Once we do that, we can update our dataETag to be the dataETag reported in 
              // the returned ChangeSetList.
              ChangeSetList changeSets = sc.getSynchronizer().getChangeSets(tableResource, firstDataETag);
              Set<String> otherChangeSets = new HashSet<String>(changeSets.getChangeSets());
              otherChangeSets.removeAll(dataETagsFromAlterations);
              
              if ( !otherChangeSets.isEmpty() ) {
                // fetch the local database so that it is again current...
                {
                  SQLiteDatabase db = null;
  
                  try {
                    db = sc.getDatabase();
                    localDataTable = ODKDatabaseUtils.get().rawSqlQuery(db, sc.getAppName(), tableId,
                        orderedColumns, null, null, null, null, DataTableColumns.ID, "ASC");
                  } finally {
                    if (db != null) {
                      db.close();
                      db = null;
                    }
                  }
                }
                
                // apply these changes
                // these changes will be local changes.
                for ( String changeSetDataETag : otherChangeSets ) {
                  websafeResumeCursor = null;
                  for (;;) {
                    RowResourceList rows = null;
                    
                    try {
                      rows = sc.getSynchronizer().getChangeSet(tableResource, changeSetDataETag, true, websafeResumeCursor);
                    } catch (Exception e) {
                      String msg = e.getMessage();
                      if (msg == null) {
                        msg = e.toString();
                      }
                      success = false;
                      tableResult.setMessage(msg);
                      tableResult.setStatus(Status.EXCEPTION);
                      return;
                    }
        
                    localDataTable = updateLocalRowsFromServerChanges(tableResource, te, orderedColumns, displayName, 
                        deferInstanceAttachments, fileAttachmentColumns, rowsToPushFileAttachments, localDataTable, rows);
                    
                    if ( rows.isHasMoreResults() ) {
                      websafeResumeCursor = rows.getWebSafeResumeCursor();
                    } else {
                      break;
                    }
                  }
                  
                }
              }
              
              // OK. now once again scan the fileAttachments...
              count = 0;
              for (SyncRowPending syncRowPending : rowsToPushFileAttachments) {
                boolean outcome = true;
                if (!syncRowPending.onlyGetFiles()) {
                  outcome = sc.getSynchronizer().putFileAttachments(tableResource.getInstanceFilesUri(),
                      tableId, syncRowPending, deferInstanceAttachments);
                }
                if (outcome) {
                  outcome = sc.getSynchronizer().getFileAttachments(tableResource.getInstanceFilesUri(),
                      tableId, syncRowPending, deferInstanceAttachments);

                  if (syncRowPending.updateSyncState()) {
                    if (outcome) {
                      // OK -- we succeeded in putting/getting all attachments
                      // update our state to the synced state.
                      SQLiteDatabase db = null;

                      try {
                        db = sc.getDatabase();
                        ODKDatabaseUtils.get().updateRowETagAndSyncState(db, tableId,
                            syncRowPending.getRowId(), syncRowPending.getRowETag(),
                            SyncState.synced);
                      } finally {
                        if (db != null) {
                          db.close();
                          db = null;
                        }
                      }
                    } else {
                      // only care about instance file status if we are trying
                      // to update state
                      instanceFileSuccess = false;
                    }
                  }
                }
                tableResult.incLocalAttachmentRetries();
                ++count;
                ++rowsProcessed;
                if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
                  sc.updateNotification(SyncProgressState.ROWS,
                      R.string.uploading_attachments_server_row, new Object[] { tableId, count,
                          rowsToPushFileAttachments.size() }, 10.0 + rowsProcessed
                          * perRowIncrement, false);
                }
              }

              // And, finally, if we get here, we can advance our 
              // local dataETag to the dataETag returned in the ChangeSetList
              if (success) {
                SQLiteDatabase db = null;

                try {
                  db = sc.getDatabase();
                  // update the dataETag to the one returned by the first
                  // of the fetch queries, above.
                  ODKDatabaseUtils.get().updateDBTableETags(db, tableId,
                      tableResource.getSchemaETag(), changeSets.getDataETag());
                  // and be sure to update our in-memory objects...
                  te.setSchemaETag(tableResource.getSchemaETag());
                  te.setLastDataETag(changeSets.getDataETag());
                  tableResource.setDataETag(changeSets.getDataETag());
                } finally {
                  if (db != null) {
                    db.close();
                    db = null;
                  }
                }
              }

              serverSuccess = true;
            } catch (ClientWebException e) {
              clientWebException("synchronizeTableRest", tableId, e, tableResult);
              serverSuccess = false;
            } catch (Exception e) {
              exception("synchronizeTableRest", tableId, e, tableResult);
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

          } catch (ClientWebException e) {
            clientWebException("synchronizeTableRest--nonMediaFiles", tableId, e, tableResult);
            log.e(TAG, "[synchronizeTableRest] error synchronizing table files");
            success = false;
          } catch (Exception e) {
            exception("synchronizeTableRest--nonMediaFiles", tableId, e, tableResult);
            log.e(TAG, "[synchronizeTableRest] error synchronizing table files");
            success = false;
          }
        }
      }

      if (success) {
        // update the last-sync-time
        // but if the table was deleted, do nothing
        SQLiteDatabase db = null;
        try {
          db = sc.getDatabase();
          if (ODKDatabaseUtils.get().hasTableId(db, tableId)) {
            ODKDatabaseUtils.get().updateDBTableLastSyncTime(db, tableId);
          }
        } finally {
          if (db != null) {
            db.close();
            db = null;
          }
        }
      }
    } finally {
      // Here we also want to add the TableResult to the value.
      if (success) {
        // Then we should have updated the db and shouldn't have set the
        // TableResult to be exception.
        if (tableResult.getStatus() != Status.WORKING) {
          log.e(TAG, "tableResult status for table: " + tableId + " was "
              + tableResult.getStatus().name()
              + ", and yet success returned true. This shouldn't be possible.");
        } else {
          if (containsConflicts) {
            tableResult.setStatus(Status.TABLE_CONTAINS_CONFLICTS);
            sc.updateNotification(SyncProgressState.ROWS,
                R.string.table_data_sync_with_conflicts, new Object[] { tableId }, 100.0, false);
          } else if (!instanceFileSuccess) {
            tableResult.setStatus(Status.TABLE_PENDING_ATTACHMENTS);
            sc.updateNotification(SyncProgressState.ROWS,
                R.string.table_data_sync_pending_attachments, new Object[] { tableId }, 100.0,
                false);
          } else {
            tableResult.setStatus(Status.SUCCESS);
            sc.updateNotification(SyncProgressState.ROWS, R.string.table_data_sync_complete,
                new Object[] { tableId }, 100.0, false);
          }
        }
      }
    }
  }
  
  private int processRowOutcomes(TableDefinitionEntry te, TableResource resource,
      TableResult tableResult, ArrayList<ColumnDefinition> orderedColumns,
      ArrayList<ColumnDefinition> fileAttachmentColumns, boolean hasAttachments,
      List<SyncRowPending> rowsToPushFileAttachments, int countSoFar, int totalOutcomesSize,
      List<SyncRow> segmentAlter, ArrayList<RowOutcome> outcomes, ArrayList<RowOutcome> specialCases) {

    ArrayList<SyncRowDataChanges> rowsToMoveToInConflictLocally = new ArrayList<SyncRowDataChanges>();
    String lastDataETag = null;

    //
    // For speed, do this all within a transaction. Processing is
    // all in-memory except when we are deleting a client row. In that
    // case, there may be SDCard access to delete the attachments for
    // the client row. But that is local access, and the commit will
    // be accessing the same device.
    //
    // i.e., no network access in this code, so we can place it all within
    // a transaction and not lock up the database for very long.
    //

    SQLiteDatabase db = null;

    try {
      db = sc.getDatabase();
      db.beginTransaction();

      for (int i = 0; i < segmentAlter.size(); ++i) {
        RowOutcome r = outcomes.get(i);
        SyncRow syncRow = segmentAlter.get(i);
        if (!r.getRowId().equals(syncRow.getRowId())) {
          throw new IllegalStateException("Unexpected reordering of return");
        }
        if (r.getOutcome() == OutcomeType.SUCCESS) {

          lastDataETag = r.getDataETagAtModification();

          if (r.isDeleted()) {
            // DELETE
            // move the local record into the 'new_row' sync state
            // so it can be physically deleted.
            ODKDatabaseUtils.get().updateRowETagAndSyncState(db, resource.getTableId(),
                r.getRowId(), null, SyncState.new_row);
            // !!Important!! update the rowETag in our copy of this row.
            syncRow.setRowETag(r.getRowETag());
            // and physically delete row and attachments from database.
            ODKDatabaseUtils.get().deleteDataInExistingDBTableWithId(db, sc.getAppName(),
                resource.getTableId(), r.getRowId());
            tableResult.incServerDeletes();
          } else {
            ODKDatabaseUtils
                .get()
                .updateRowETagAndSyncState(
                    db,
                    resource.getTableId(),
                    r.getRowId(),
                    r.getRowETag(),
                    (hasAttachments && !syncRow.getUriFragments().isEmpty()) ? SyncState.synced_pending_files
                        : SyncState.synced);
            // !!Important!! update the rowETag in our copy of this row.
            syncRow.setRowETag(r.getRowETag());
            if (hasAttachments && !syncRow.getUriFragments().isEmpty()) {
              rowsToPushFileAttachments.add(new SyncRowPending(syncRow, false, true, true));
            }
            // UPDATE or INSERT
            tableResult.incServerUpserts();
          }
        } else if (r.getOutcome() == OutcomeType.FAILED) {
          if (r.getRowId() == null || !r.isDeleted()) {
            // should never occur!!!
            throw new IllegalStateException(
                "Unexpected null rowId or OutcomeType.FAILED when not deleting row");
          } else {
            // special case of a delete where server has no record of the row.
            // server should add row and mark it as deleted.
          }
        } else if (r.getOutcome() == OutcomeType.IN_CONFLICT) {
          // another device updated this record between the time we fetched
          // changes
          // and the time we tried to update this record. Transition the record
          // locally into the conflicting state.
          // SyncState.deleted and server is not deleting
          // SyncState.new_row and record exists on server
          // SyncState.changed and new change on server
          // SyncState.in_conflict and new change on server

          // no need to worry about server in_conflict records.
          // any server in_conflict rows will be cleaned up during the
          // update of the in_conflict state.
          Integer localRowConflictType = syncRow.isDeleted() ? ConflictType.LOCAL_DELETED_OLD_VALUES
              : ConflictType.LOCAL_UPDATED_UPDATED_VALUES;

          Integer serverRowConflictType = r.isDeleted() ? ConflictType.SERVER_DELETED_OLD_VALUES
              : ConflictType.SERVER_UPDATED_UPDATED_VALUES;

          // figure out what the localRow conflict type sh
          SyncRow serverRow = new SyncRow(r.getRowId(), r.getRowETag(), r.isDeleted(),
              r.getFormId(), r.getLocale(), r.getSavepointType(), r.getSavepointTimestamp(),
              r.getSavepointCreator(), r.getFilterScope(), r.getValues(), fileAttachmentColumns);
          SyncRowDataChanges conflictRow = new SyncRowDataChanges(serverRow, syncRow, false, localRowConflictType);

          rowsToMoveToInConflictLocally.add(conflictRow);
          // we transition all of these later, outside this processing loop...
        } else if (r.getOutcome() == OutcomeType.DENIED) {
          // user does not have privileges...
          specialCases.add(r);
        } else {
          // a new OutcomeType state was added!
          throw new IllegalStateException("Unexpected OutcomeType! " + r.getOutcome().name());
        }

        ++countSoFar;
        ++rowsProcessed;
        if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
          sc.updateNotification(SyncProgressState.ROWS, R.string.altering_server_row,
              new Object[] { resource.getTableId(), countSoFar, totalOutcomesSize }, 10.0
                  + rowsProcessed * perRowIncrement, false);
        }
      }

      // process the conflict rows, if any
      conflictRowsInDb(db, resource, orderedColumns, rowsToMoveToInConflictLocally,
          rowsToPushFileAttachments, hasAttachments, tableResult);

      // and update the lastDataETag that we hold
      // TODO: BUG BUG BUG BUG BUG
      // TODO: BUG BUG BUG BUG BUG
      // TODO: BUG BUG BUG BUG BUG
      // TODO: BUG BUG BUG BUG BUG
      // TODO: BUG BUG BUG BUG BUG
      if (lastDataETag != null) {
        // TODO: timing window here!!!!
        // another updater could have interleaved a dataETagAtModification
        // value.
        // so updating the lastDataETag here could cause us to miss those
        // updates!!!

        ODKDatabaseUtils.get().updateDBTableETags(db, resource.getTableId(), te.getSchemaETag(),
            lastDataETag);

        resource.setDataETag(lastDataETag);
        te.setLastDataETag(lastDataETag);
      }
      // and allow this to happen
      db.setTransactionSuccessful();
    } finally {
      if (db != null) {
        db.endTransaction();
        db.close();
        db = null;
      }
    }

    return countSoFar;
  }

  /**
   * Delete any pre-existing server conflict records for the list of rows (changes).
   * If the server and local rows are both deletes, delete the local row (and 
   * its attachments), thereby completing the deletion of the row (entirely).
   * Otherwise, change the local row to the in_conflict state, and insert a copy
   * of the server row locally, configured as a server conflict record; in that 
   * case, add the server and client rows to rowsToSyncFileAttachments.
   * 
   * @param db
   * @param resource
   * @param orderedColumns
   * @param changes
   * @param rowsToSyncFileAttachments
   * @param hasAttachments
   * @param tableResult
   * @throws ClientWebException
   */
  private void conflictRowsInDb(SQLiteDatabase db, TableResource resource,
      ArrayList<ColumnDefinition> orderedColumns, List<SyncRowDataChanges> changes,
      List<SyncRowPending> rowsToSyncFileAttachments, boolean hasAttachments,
      TableResult tableResult) throws ClientWebException {

    int count = 0;
    for (SyncRowDataChanges change : changes) {
      SyncRow serverRow = change.serverRow;
      log.i(TAG,
          "conflicting row, id=" + serverRow.getRowId() + " rowETag=" + serverRow.getRowETag());
      ContentValues values = new ContentValues();

      // delete the old server-values in_conflict row if it exists
      ODKDatabaseUtils.get().deleteServerConflictRowWithId(db, resource.getTableId(),
          serverRow.getRowId());
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

        // special case -- the server and local rows are both being deleted --
        // just delete them!

        // move the local record into the 'new_row' sync state
        // so it can be physically deleted.
        ODKDatabaseUtils.get().updateRowETagAndSyncState(db, resource.getTableId(),
            serverRow.getRowId(), null, SyncState.new_row);
        // and physically delete it.
        ODKDatabaseUtils.get().deleteDataInExistingDBTableWithId(db, sc.getAppName(),
            resource.getTableId(), serverRow.getRowId());

        tableResult.incLocalDeletes();
      } else {
        // update the localRow to be in_conflict
        ODKDatabaseUtils.get().placeRowIntoConflict(db, resource.getTableId(),
            serverRow.getRowId(), localRowConflictType);

        // set up to insert the in_conflict row from the server
        for (DataKeyValue entry : serverRow.getValues()) {
          String colName = entry.column;
          values.put(colName, entry.value);
        }

        // insert in_conflict server row
        values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
        values.put(DataTableColumns.SYNC_STATE, SyncState.in_conflict.name());
        values.put(DataTableColumns.CONFLICT_TYPE, serverRowConflictType);
        values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
        values.put(DataTableColumns.LOCALE, serverRow.getLocale());
        values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
        values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());
        Scope.Type type = serverRow.getFilterScope().getType();
        values.put(DataTableColumns.FILTER_TYPE,
            (type == null) ? Scope.Type.DEFAULT.name() : type.name());
        values.put(DataTableColumns.FILTER_VALUE, serverRow.getFilterScope().getValue());
        ODKDatabaseUtils.get().insertDataIntoExistingDBTableWithId(db, resource.getTableId(),
            orderedColumns, values, serverRow.getRowId());

        // We're going to check our representation invariant here. A local and
        // a server version of the row should only ever be changed/changed,
        // deleted/changed, or changed/deleted. Anything else and we're in
        // trouble.
        if (localRowConflictType == ConflictType.LOCAL_DELETED_OLD_VALUES
            && serverRowConflictType != ConflictType.SERVER_UPDATED_UPDATED_VALUES) {
          log.e(TAG, "local row conflict type is local_deleted, but server "
              + "row conflict_type is not server_udpated. These states must"
              + " go together, something went wrong.");
        } else if (localRowConflictType != ConflictType.LOCAL_UPDATED_UPDATED_VALUES) {
          log.e(TAG, "localRowConflictType was not local_deleted or "
              + "local_updated! this is an error. local conflict type: " + localRowConflictType
              + ", server conflict type: " + serverRowConflictType);
        }

        tableResult.incLocalConflicts();

        // try to pull the file attachments for the in_conflict rows
        // it is OK if we can't get them, but they may be useful for
        // reconciliation
        if (hasAttachments) {
          if (!change.localRow.getUriFragments().isEmpty()) {
            rowsToSyncFileAttachments.add(new SyncRowPending(change.localRow, true, false, false));
          }
          if (!serverRow.getUriFragments().isEmpty()) {
            rowsToSyncFileAttachments.add(new SyncRowPending(serverRow, true, false, false));
          }
        }
      }
      ++count;
      ++rowsProcessed;
      if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
        sc.updateNotification(SyncProgressState.ROWS, R.string.marking_conflicting_local_row,
            new Object[] { resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed
                * perRowIncrement, false);
      }
    }
  }

  /**
   * Inserts the given list of rows (changes) into the local database.
   * Adds those rows to the rowsToPushFileAttachments list if they have
   * any non-null media attachments.
   * 
   * @param db
   * @param resource
   * @param orderedColumns
   * @param changes
   * @param rowsToPushFileAttachments
   * @param hasAttachments
   * @param tableResult
   * @throws ClientWebException
   */
  private void insertRowsInDb(SQLiteDatabase db, TableResource resource,
      ArrayList<ColumnDefinition> orderedColumns, List<SyncRowDataChanges> changes,
      List<SyncRowPending> rowsToPushFileAttachments, boolean hasAttachments,
      TableResult tableResult) throws ClientWebException {
    int count = 0;
    for (SyncRowDataChanges change : changes) {
      SyncRow serverRow = change.serverRow;
      ContentValues values = new ContentValues();

      values.put(DataTableColumns.ID, serverRow.getRowId());
      values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
      values.put(DataTableColumns.SYNC_STATE, (hasAttachments && !serverRow.getUriFragments()
          .isEmpty()) ? SyncState.synced_pending_files.name() : SyncState.synced.name());
      values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
      values.put(DataTableColumns.LOCALE, serverRow.getLocale());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
      values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());

      for (DataKeyValue entry : serverRow.getValues()) {
        String colName = entry.column;
        values.put(colName, entry.value);
      }

      ODKDatabaseUtils.get().insertDataIntoExistingDBTableWithId(db, resource.getTableId(),
          orderedColumns, values, serverRow.getRowId());
      tableResult.incLocalInserts();

      if (hasAttachments && !serverRow.getUriFragments().isEmpty()) {
        rowsToPushFileAttachments.add(new SyncRowPending(serverRow, true, true, true));
      }
      ++count;
      ++rowsProcessed;
      if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
        sc.updateNotification(SyncProgressState.ROWS, R.string.inserting_local_row, new Object[] {
            resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement,
            false);
      }
    }
  }

  /**
   * Updates the given list of rows (changes) in the local database.
   * Adds those rows to the rowsToPushFileAttachments list if they have
   * any non-null media attachments.
   * 
   * @param db
   * @param resource
   * @param orderedColumns
   * @param changes
   * @param rowsToSyncFileAttachments
   * @param hasAttachments
   * @param tableResult
   * @throws ClientWebException
   */
  private void updateRowsInDb(SQLiteDatabase db, TableResource resource,
      ArrayList<ColumnDefinition> orderedColumns, List<SyncRowDataChanges> changes,
      List<SyncRowPending> rowsToSyncFileAttachments, boolean hasAttachments,
      TableResult tableResult) throws ClientWebException {
    int count = 0;
    for (SyncRowDataChanges change : changes) {
      // if the localRow sync state was synced_pending_files,
      // ensure that all those files are uploaded before
      // we update the row. This ensures that all attachments
      // are saved before we revise the local row value.
      if (change.isRestPendingFiles) {
        log.w(TAG,
            "file attachment at risk -- updating from server while in synced_pending_files state. rowId: "
                + change.localRow.getRowId() + " rowETag: " + change.localRow.getRowETag());
      }

      // update the row from the changes on the server
      SyncRow serverRow = change.serverRow;
      ContentValues values = new ContentValues();

      values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
      values.put(DataTableColumns.SYNC_STATE, (hasAttachments && !serverRow.getUriFragments()
          .isEmpty()) ? SyncState.synced_pending_files.name() : SyncState.synced.name());
      values.put(DataTableColumns.FILTER_TYPE, serverRow.getFilterScope().getType().name());
      values.put(DataTableColumns.FILTER_VALUE, serverRow.getFilterScope().getValue());
      values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
      values.put(DataTableColumns.LOCALE, serverRow.getLocale());
      values.put(DataTableColumns.SAVEPOINT_TYPE, serverRow.getSavepointType());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
      values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());

      for (DataKeyValue entry : serverRow.getValues()) {
        String colName = entry.column;
        values.put(colName, entry.value);
      }

      ODKDatabaseUtils.get().updateDataInExistingDBTableWithId(db, resource.getTableId(),
          orderedColumns, values, serverRow.getRowId());
      tableResult.incLocalUpdates();

      if (hasAttachments && !serverRow.getUriFragments().isEmpty()) {
        rowsToSyncFileAttachments.add(new SyncRowPending(serverRow, false, true, true));
      }

      ++count;
      ++rowsProcessed;
      if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
        sc.updateNotification(SyncProgressState.ROWS, R.string.updating_local_row, new Object[] {
            resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement,
            false);
      }
    }
  }

  /**
   * Attempt to push all the attachments of the local rows up to the server
   * (before the row is locally deleted). If the attachments were pushed to the
   * server, the 'isRestPendingFiles' flag is cleared. This makes the local row
   * eligible for deletion.  Otherwise, the localRow is removed from the 
   * localRowplaced in the 
   * 
   * @param db
   * @param resource
   * @param tableId
   * @param changes
   * @param fileAttachmentColumns
   * @param deferInstanceAttachments
   * @param tableResult
   * @return
   * @throws IOException
   */
  private void pushLocalAttachmentsBeforeDeleteRowsInDb(SQLiteDatabase db,
      TableResource resource, List<SyncRowDataChanges> changes,
      ArrayList<ColumnDefinition> fileAttachmentColumns, boolean deferInstanceAttachments,
      TableResult tableResult) throws IOException {

    boolean deletesAllSuccessful = true;

    // try first to push any attachments of the soon-to-be-deleted
    // local row up to the server
    for ( int i = 0 ; i < changes.size() ; ) {
      SyncRowDataChanges change = changes.get(i);
      if (change.isRestPendingFiles) {
        if (change.localRow.getUriFragments().isEmpty()) {
          // nothing to push
          change.isRestPendingFiles = false;
          ++i;
        } else {
          // since we are directly calling putFileAttachments, the flags in this
          // constructor are never accessed. Use false for their values.
          SyncRowPending srp = new SyncRowPending(change.localRow, false, false, false);
          boolean outcome = sc.getSynchronizer().putFileAttachments(resource.getInstanceFilesUri(),
              resource.getTableId(), srp, deferInstanceAttachments);
          if (outcome) {
            // successful
            change.isRestPendingFiles = false;
            ++i;
          } else {
            // there are files that should be pushed that weren't.
            // change local state to deleted, and remove from the 
            // this list.
            ODKDatabaseUtils.get().updateRowETagAndSyncState(db, resource.getTableId(),
                change.localRow.getRowId(), change.serverRow.getRowETag(), SyncState.deleted);
            changes.remove(i);
          }
        }
      } else {
        ++i;
      }
    }
  }

  /**
   * Delete the rows that have had all of their (locally-available) attachments
   * pushed to the server. I.e., those with 'isRestPendingFiles' false. Otherwise,
   * leave these rows in the local database until their files are pushed and they
   * can safely be removed.
   * 
   * @param db
   * @param resource
   * @param changes
   * @param fileAttachmentColumns
   * @param deferInstanceAttachments
   * @param tableResult
   * @throws IOException
   */
  private void deleteRowsInDb(SQLiteDatabase db, TableResource resource, List<SyncRowDataChanges> changes,
      ArrayList<ColumnDefinition> fileAttachmentColumns, boolean deferInstanceAttachments,
      TableResult tableResult) throws IOException {
    int count = 0;

    // now delete the rows we can delete...
    for (SyncRowDataChanges change : changes) {
      if (!change.isRestPendingFiles) {
        // DELETE
        // move the local record into the 'new_row' sync state
        // so it can be physically deleted.
        ODKDatabaseUtils.get().updateRowETagAndSyncState(db, resource.getTableId(),
            change.serverRow.getRowId(), null, SyncState.new_row);
        // and physically delete row and attachments from database.
        ODKDatabaseUtils.get().deleteDataInExistingDBTableWithId(db, sc.getAppName(),
            resource.getTableId(), change.serverRow.getRowId());
        tableResult.incLocalDeletes();
      }
      ++count;
      ++rowsProcessed;
      if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
        sc.updateNotification(SyncProgressState.ROWS, R.string.deleting_local_row, new Object[] {
            resource.getTableId(), count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement,
            false);
      }
    }
  }
}
