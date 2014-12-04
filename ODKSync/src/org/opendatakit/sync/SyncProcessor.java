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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.wink.client.ClientWebException;
import org.opendatakit.aggregate.odktables.rest.ConflictType;
import org.opendatakit.aggregate.odktables.rest.SyncState;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome.OutcomeType;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.Scope;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinitionResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.ElementDataType;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.data.UserTable;
import org.opendatakit.common.android.data.UserTable.Row;
import org.opendatakit.common.android.database.DatabaseFactory;
import org.opendatakit.common.android.provider.DataTableColumns;
import org.opendatakit.common.android.utilities.CsvUtil;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;
import org.opendatakit.common.android.utilities.ODKFileUtils;
import org.opendatakit.common.android.utilities.TableUtil;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.sync.SynchronizationResult.Status;
import org.opendatakit.sync.Synchronizer.OnTablePropertiesChanged;
import org.opendatakit.sync.Synchronizer.SynchronizerStatus;
import org.opendatakit.sync.exceptions.SchemaMismatchException;
import org.opendatakit.sync.service.SyncNotification;
import org.opendatakit.sync.service.SyncProgressState;

import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * SyncProcessor implements the cloud synchronization logic for Tables.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public class SyncProcessor implements SynchronizerStatus {

  private static final String TAG = SyncProcessor.class.getSimpleName();

  private static final int UPSERT_BATCH_SIZE = 500;
  private static final int ROWS_BETWEEN_PROGRESS_UPDATES = 10;
  private static final int OVERALL_PROGRESS_BAR_LENGTH = 6350400;
  private static final ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    mapper.setVisibilityChecker(mapper.getVisibilityChecker().withFieldVisibility(Visibility.ANY));
  }

  private WebLogger log;
  private int nMajorSyncSteps;
  private int iMajorSyncStep;
  private int GRAINS_PER_MAJOR_SYNC_STEP;

  private final Context context;
  private final String appName;
  private final SyncNotification syncProgress;
  private final Synchronizer synchronizer;
  /**
   * The results of the synchronization that we will pass back to the user.
   */
  private final SynchronizationResult mUserResult;

  public SyncProcessor(Context context, String appName, Synchronizer synchronizer,
      SyncNotification syncProgress) {
    this.context = context;
    this.appName = appName;
    this.log = WebLogger.getLogger(appName);
    this.syncProgress = syncProgress;
    this.synchronizer = synchronizer;
    this.mUserResult = new SynchronizationResult();
  }

  public SynchronizationResult getOverallResults() {
    return mUserResult;
  }

  @Override
  public void updateNotification(SyncProgressState state, int textResource, Object[] formatArgVals,
      Double progressPercentage, boolean indeterminateProgress) {
    String text = "Bad text resource id: " + textResource + "!";
    String fmt = this.context.getString(textResource);
    if (fmt != null) {
      if (formatArgVals == null) {
        text = fmt;
      } else {
        text = String.format(fmt, formatArgVals);
      }
    }
    syncProgress.updateNotification(state, text, OVERALL_PROGRESS_BAR_LENGTH, (int) (iMajorSyncStep
        * GRAINS_PER_MAJOR_SYNC_STEP + ((progressPercentage != null) ? (progressPercentage
        * GRAINS_PER_MAJOR_SYNC_STEP / 100.0) : 0.0)), indeterminateProgress);
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
    log.i(TAG, "entered synchronizeConfigurationAndContent()");
    ODKFileUtils.assertDirectoryStructure(appName);
    boolean issueDeletes = false;
    if ( SyncApp.getInstance().shouldWaitForDebugger() ) {
      issueDeletes = true;
      android.os.Debug.waitForDebugger();
    }

    syncProgress.updateNotification(SyncProgressState.STARTING,
        context.getString(R.string.retrieving_tables_list_from_server),
        OVERALL_PROGRESS_BAR_LENGTH, 0, false);

    // get tables (tableId -> schemaETag) from server
    List<TableResource> tables = new ArrayList<TableResource>();
    try {
      tables = synchronizer.getTables();
      if (tables == null) {
        tables = new ArrayList<TableResource>();
      }
    } catch (ClientWebException e) {
      mUserResult.setAppLevelStatus(Status.AUTH_EXCEPTION);
      log.i(TAG,
          "[synchronizeConfigurationAndContent] Could not retrieve server table list exception: "
              + e.toString());
      return;
    } catch (Exception e) {
      mUserResult.setAppLevelStatus(Status.EXCEPTION);
      log.e(TAG,
          "[synchronizeConfigurationAndContent] Unexpected exception getting server table list exception: "
              + e.toString());
      return;
    }

    // TODO: do the database updates with a few database transactions...

    // get the tables on the local device
    List<String> localTableIds;
    SQLiteDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(context, appName);
      localTableIds = ODKDatabaseUtils.get().getAllTableIds(db);
    } catch (SQLiteException e) {
      mUserResult.setAppLevelStatus(Status.EXCEPTION);
      log.e(TAG,
          "[synchronizeConfigurationAndContent] Unexpected exception getting local tableId list exception: "
              + e.toString());
      return;
    } finally {
      if (db != null) {
        db.close();
        db = null;
      }
    }

    // Figure out how many major steps there are to the sync
    {
      Set<String> uniqueTableIds = new HashSet<String>();
      uniqueTableIds.addAll(localTableIds);
      for (TableResource table : tables) {
        uniqueTableIds.add(table.getTableId());
      }
      // when pushing, we never drop tables on the server (but never pull those
      // either).
      // i.e., pushing only adds to the set of tables on the server.
      //
      // when pulling, we drop all local tables that do not match the server,
      // and pull
      // everything from the server.
      nMajorSyncSteps = 1 + (pushToServer ? 2 * localTableIds.size()
          : (uniqueTableIds.size() + tables.size()));
      GRAINS_PER_MAJOR_SYNC_STEP = (OVERALL_PROGRESS_BAR_LENGTH / nMajorSyncSteps);
    }
    iMajorSyncStep = 0;

    // TODO: fix sync sequence
    // TODO: fix sync sequence
    // TODO: fix sync sequence
    // TODO: fix sync sequence
    // TODO: fix sync sequence
    // TODO: fix sync sequence
    // Intermediate deployment failures can leave the client in a bad state.
    // The actual sync protocol should probably be:
    //
    // (1) pull down all the table-id level file changes and new files
    // (2) pull down all the app-level file changes and new files
    // (3) delete the app-level files locally
    // (4) delete the table-id level files locally
    //
    // We also probably want some critical files to be pulled last. e.g.,
    // tables/tableid/index.html , assets/index.html ?
    // so that we know that all supporting files are present before we
    // update these files.
    //
    // As long as form changes are done via completely new form ids, and
    // push as new form id files, this enables the sync to pull the new forms,
    // then presumably the table-level files would control the launching of
    // those forms, and the app-level files would launch the table-level files
    //

    // First we're going to synchronize the app level files.
    try {
      boolean success = synchronizer.syncAppLevelFiles(pushToServer, this);
      mUserResult.setAppLevelStatus(success ? Status.SUCCESS : Status.FAILURE);
    } catch (ClientWebException e) {
      // TODO: update a synchronization result to report back to them as well.
      mUserResult.setAppLevelStatus(Status.AUTH_EXCEPTION);
      log.e(TAG,
          "[synchronizeConfigurationAndContent] error trying to synchronize app-level files.");
      log.printStackTrace(e);
      return;
    }

    // done with app-level file synchronization
    ++iMajorSyncStep;

    if (pushToServer) {
      Set<TableResource> serverTablesToDelete = new HashSet<TableResource>();
      serverTablesToDelete.addAll(tables);
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
        log.i(TAG, "[synchronizeConfigurationAndContent] synchronizing table " + localTableId);

        if (!localTableId.equals("framework")) {
          TableDefinitionEntry entry;
          ArrayList<ColumnDefinition> orderedDefns;
          try {
            db = DatabaseFactory.get().getDatabase(context, appName);
            entry = ODKDatabaseUtils.get().getTableDefinitionEntry(db, localTableId);
            orderedDefns = TableUtil.get().getColumnDefinitions(db, appName, localTableId);
          } finally {
            if (db != null) {
              db.close();
              db = null;
            }
          }

          if ( matchingResource != null ) {
            serverTablesToDelete.remove(matchingResource);
          }
          // do not sync the framework table
          synchronizeTableConfigurationAndContent(entry, orderedDefns, matchingResource, true);
        }

        this.updateNotification(SyncProgressState.TABLE_FILES,
            R.string.table_level_file_sync_complete, new Object[] { localTableId }, 100.0, false);
        ++iMajorSyncStep;
      }
      
      // TODO: make this configurable?
      // Generally should not allow this, as it is very dangerous
      // delete any other tables 
      if ( issueDeletes ) {
        for ( TableResource tableToDelete : serverTablesToDelete ) {
          synchronizer.deleteTable(tableToDelete);
        }
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
      // do not remove the framework table
      localTableIdsToDelete.remove("framework");

      --iMajorSyncStep;
      for (TableResource table : tables) {
        ++iMajorSyncStep;

        ArrayList<ColumnDefinition> orderedDefns = null;

        String serverTableId = table.getTableId();

        boolean doesNotExistLocally = true;
        boolean isLocalMatch = false;
        TableDefinitionEntry entry = null;

        if (localTableIds.contains(serverTableId)) {
          localTableIdsToDelete.remove(serverTableId);
          doesNotExistLocally = false;

          // see if the schemaETag matches. If so, we can skip a lot of steps...
          // no need to verify schema match -- just sync files...
          try {
            db = DatabaseFactory.get().getDatabase(context, appName);
            entry = ODKDatabaseUtils.get().getTableDefinitionEntry(db, serverTableId);
            orderedDefns = TableUtil.get().getColumnDefinitions(db, appName, serverTableId);
            if (table.getSchemaETag().equals(entry.getSchemaETag())) {
              isLocalMatch = true;
            }
          } catch (SQLiteException e) {
            mUserResult.setAppLevelStatus(Status.EXCEPTION);
            log.e(TAG,
                "[synchronizeConfigurationAndContent] Unexpected exception getting columns of tableId: "
                    + serverTableId + " exception: " + e.toString());
            return;
          } finally {
            if (db != null) {
              db.close();
              db = null;
            }
          }
        }

        TableResult tableResult = mUserResult.getTableResult(serverTableId);

        updateNotification(SyncProgressState.TABLE_FILES,
            (doesNotExistLocally ? R.string.creating_local_table
                : R.string.verifying_table_schema_on_server), new Object[] { serverTableId }, 0.0,
            false);

        if (!isLocalMatch) {
          try {
            TableDefinitionResource definitionResource = synchronizer.getTableDefinition(table
                .getDefinitionUri());

            try {
              db = DatabaseFactory.get().getDatabase(context, appName);
              orderedDefns = addTableFromDefinitionResource(db, definitionResource,
                  doesNotExistLocally);
              entry = ODKDatabaseUtils.get().getTableDefinitionEntry(db, serverTableId);
            } finally {
              if (db != null) {
                db.close();
                db = null;
              }
            }
          } catch (JsonParseException e) {
            log.printStackTrace(e);
            tableResult.setStatus(Status.EXCEPTION);
            log.e(TAG,
                "[synchronizeConfigurationAndContent] Unexpected exception parsing table definition exception: "
                    + e.toString());
            continue;
          } catch (JsonMappingException e) {
            log.printStackTrace(e);
            tableResult.setStatus(Status.EXCEPTION);
            log.e(TAG,
                "[synchronizeConfigurationAndContent] Unexpected exception parsing table definition exception: "
                    + e.toString());
            continue;
          } catch (IOException e) {
            log.printStackTrace(e);
            tableResult.setStatus(Status.EXCEPTION);
            log.e(TAG,
                "[synchronizeConfigurationAndContent] Unexpected exception accessing table definition exception: "
                    + e.toString());
            continue;
          } catch (SchemaMismatchException e) {
            log.printStackTrace(e);
            tableResult.setStatus(Status.EXCEPTION);
            log.e(TAG,
                "[synchronizeConfigurationAndContent] The schema for this table does not match that on the server"
                    + e.toString());
            continue;
          }
        }

        // Sync the local media files with the server if the table
        // existed locally before we attempted downloading it.

        synchronizeTableConfigurationAndContent(entry, orderedDefns, table, false);
        this.updateNotification(SyncProgressState.TABLE_FILES,
            R.string.table_level_file_sync_complete, new Object[] { serverTableId }, 100.0, false);
      }
      ++iMajorSyncStep;

      // and now loop through the ones to delete...
      for (String localTableId : localTableIdsToDelete) {
        updateNotification(SyncProgressState.TABLE_FILES, R.string.dropping_local_table,
            new Object[] { localTableId }, 0.0, false);
        // eventually might not be true if there are multiple syncs running
        // simultaneously...
        TableResult tableResult = mUserResult.getTableResult(localTableId);
        try {
          db = DatabaseFactory.get().getDatabase(context, appName);
          ODKDatabaseUtils.get().deleteDBTableAndAllData(db, appName, localTableId);
          tableResult.setStatus(Status.SUCCESS);
        } catch (SQLiteException e) {
          tableResult.setStatus(Status.EXCEPTION);
          log.e(TAG,
              "[synchronizeConfigurationAndContent] Unexpected exception deleting local tableId "
                  + localTableId + " exception: " + e.toString());
        } catch (Exception e) {
          tableResult.setStatus(Status.EXCEPTION);
          log.e(TAG,
              "[synchronizeConfigurationAndContent] Unexpected exception deleting local tableId "
                  + localTableId + " exception: " + e.toString());
        } finally {
          if (db != null) {
            db.close();
            db = null;
          }
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
   * This method does NOT synchronize the framework files. The management of the
   * contents of the framework directory is managed by the individual APKs
   * themselves.
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
  private void synchronizeTableConfigurationAndContent(TableDefinitionEntry te,
      ArrayList<ColumnDefinition> orderedDefns, TableResource resource,
      boolean pushLocalTableLevelFiles) {

    // used to get the above from the ACTIVE store. if things go wonky, maybe
    // check to see if it was ACTIVE rather than SERVER for a reason. can't
    // think of one. one thing is that if it fails you'll see a table but won't
    // be able to open it, as there won't be any KVS stuff appropriate for it.
    boolean success = false;
    // Prepare the tableResult. We'll start it as failure, and only update it
    // if we're successful at the end.

    String tableId = te.getTableId();

    this.updateNotification(SyncProgressState.TABLE_FILES,
        R.string.verifying_table_schema_on_server, new Object[] { tableId }, 0.0, false);
    final TableResult tableResult = mUserResult.getTableResult(tableId);
    String displayName;
    SQLiteDatabase db = null;
    try {
      db = DatabaseFactory.get().getDatabase(context, appName);
      displayName = TableUtil.get().getLocalizedDisplayName(db, tableId);
      tableResult.setTableDisplayName(displayName);
    } finally {
      if (db != null) {
        db.close();
        db = null;
      }
    }

    try {
      String dataETag = te.getLastDataETag();
      String schemaETag = te.getSchemaETag();
      boolean serverUpdated = false;

      if (resource == null) {
        // exists locally but not on server...

        if (!pushLocalTableLevelFiles) {
          // the table on the server is missing. Need to ask user what to do...
          // we should never really get in this state (should be caught up in
          // caller)
          tableResult.setServerHadSchemaChanges(true);
          tableResult
              .setMessage("Server no longer has table! Deleting it locally. Reset App Server to upload.");
          tableResult.setStatus(Status.TABLE_DOES_NOT_EXIST_ON_SERVER);
          return;
        }

        // the insert of the table was incomplete -- try again

        // we are creating data on the server
        try {
          db = DatabaseFactory.get().getDatabase(context, appName);
          db.beginTransaction();
          // change row sync and conflict status to handle new server schema.
          // Clean up this table and set the dataETag to null.
          ODKDatabaseUtils.get().changeDataRowsToNewRowState(db, tableId);
          // we need to clear out the dataETag so
          // that we will pull all server changes and sync our properties.
          ODKDatabaseUtils.get().updateDBTableETags(db, tableId, null, null);
          db.setTransactionSuccessful();
        } finally {
          if (db != null) {
            db.endTransaction();
            db.close();
            db = null;
          }
        }

        dataETag = null;
        /**************************
         * PART 1A: CREATE THE TABLE First we need to create the table on the
         * server. This comes in two parts--the definition and the properties.
         **************************/
        // First create the table definition on the server.
        try {
          resource = synchronizer.createTable(tableId, schemaETag,
              ColumnDefinition.getColumns(orderedDefns));
        } catch (Exception e) {
          log.printStackTrace(e);
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        }

        schemaETag = resource.getSchemaETag();
        try {
          db = DatabaseFactory.get().getDatabase(context, appName);
          db.beginTransaction();
          // update schemaETag to that on server (dataETag is null already).
          ODKDatabaseUtils.get().updateDBTableETags(db, tableId, schemaETag, null);
          db.setTransactionSuccessful();
        } finally {
          if (db != null) {
            db.endTransaction();
            db.close();
            db = null;
          }
        }
        serverUpdated = true;
      }

      // we found the matching resource on the server and we have set up our
      // local table to be ready for any data merge with the server's table.

      /**************************
       * PART 1A: UPDATE THE TABLE SCHEMA. This should generally not happen. But
       * we allow a server wipe and re-install by another user with the same
       * physical schema to match ours even when our schemaETag differs. IN this
       * case, we need to mark our data as needing a full re-sync.
       **************************/
      if (!resource.getSchemaETag().equals(schemaETag)) {
        // the server was re-installed by a different device.
        // verify that our table definition is identical to the
        // server, and, if it is, update our schemaETag to match
        // the server's.

        log.d(TAG, "updateDbFromServer setServerHadSchemaChanges(true)");
        tableResult.setServerHadSchemaChanges(true);

        // fetch the table definition
        TableDefinitionResource definitionResource;
        try {
          definitionResource = synchronizer.getTableDefinition(resource.getDefinitionUri());
        } catch (Exception e) {
          log.printStackTrace(e);
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
          db = DatabaseFactory.get().getDatabase(context, appName);
          // apply changes
          // this also updates the data rows so they will sync
          orderedDefns = addTableFromDefinitionResource(db, definitionResource, false);

          log.w(TAG,
              "database schema has changed. Structural modifications, if any, were successful.");
        } catch (SchemaMismatchException e) {
          log.printStackTrace(e);
          log.w(TAG, "database properties have changed. "
              + "structural modifications were not successful. You must delete the table"
              + " and download it to receive the updates.");
          tableResult.setMessage(e.toString());
          tableResult.setStatus(Status.FAILURE);
          return;
        } catch (JsonParseException e) {
          log.printStackTrace(e);
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        } catch (JsonMappingException e) {
          log.printStackTrace(e);
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        } catch (IOException e) {
          log.printStackTrace(e);
          String msg = e.getMessage();
          if (msg == null)
            msg = e.toString();
          tableResult.setMessage(msg);
          tableResult.setStatus(Status.EXCEPTION);
          return;
        } finally {
          if (db != null) {
            db.close();
            db = null;
          }
        }
      }

      // OK. we have the schemaETag matching.

      // write our properties and definitions files.
      final CsvUtil utils = new CsvUtil(context, appName);
      // write the current schema and properties set.
      try {
        db = DatabaseFactory.get().getDatabase(context, appName);
        utils.writePropertiesCsv(db, tableId, orderedDefns);
      } finally {
        if (db != null) {
          db.close();
          db = null;
        }
      }

      synchronizer.syncTableLevelFiles(tableId, new OnTablePropertiesChanged() {
        @Override
        public void onTablePropertiesChanged(String tableId) {
          try {
            utils.updateTablePropertiesFromCsv(null, tableId);
          } catch (IOException e) {
            log.printStackTrace(e);
            String msg = e.getMessage();
            if (msg == null)
              msg = e.toString();
            tableResult.setMessage(msg);
            tableResult.setStatus(Status.EXCEPTION);
          }
        }
      }, pushLocalTableLevelFiles, this);

      // we found the matching resource on the server and we have set up our
      // local table to be ready for any data merge with the server's table.

      // we should be up-to-date on the schema and properties
      success = true;
    } finally {
      if (success && tableResult.getStatus() != Status.WORKING) {
        log.e(TAG, "tableResult status for table: " + tableId + " was "
            + tableResult.getStatus().name()
            + ", and yet success returned true. This shouldn't be possible.");
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
  public void synchronizeDataRowsAndAttachments(boolean deferInstanceAttachments) {
    log.i(TAG, "entered synchronize()");
    ODKFileUtils.assertDirectoryStructure(appName);

    if (mUserResult.getAppLevelStatus() != Status.SUCCESS) {
      log.e(TAG, "Abandoning data row update -- app-level sync was not successful!");
      return;
    }

    List<String> tableIds;
    SQLiteDatabase db = null;

    try {
      db = DatabaseFactory.get().getDatabase(context, appName);
      tableIds = ODKDatabaseUtils.get().getAllTableIds(db);
    } finally {
      if (db != null) {
        db.close();
        db = null;
      }
    }

    // we can assume that all the local table properties should
    // sync with the server.
    for (String tableId : tableIds) {
      // Sync the local media files with the server if the table
      // existed locally before we attempted downloading it.

      TableDefinitionEntry te;
      ArrayList<ColumnDefinition> orderedDefns;
      String displayName;
      try {
        db = DatabaseFactory.get().getDatabase(context, appName);
        te = ODKDatabaseUtils.get().getTableDefinitionEntry(db, tableId);
        orderedDefns = TableUtil.get().getColumnDefinitions(db, appName, tableId);
        displayName = TableUtil.get().getLocalizedDisplayName(db, tableId);
      } finally {
        if (db != null) {
          db.close();
          db = null;
        }
      }

      synchronizeTableDataRowsAndAttachments(te, orderedDefns, displayName,
          deferInstanceAttachments);
      ++iMajorSyncStep;
    }
  }

  private Double perRowIncrement;
  private int rowsProcessed;

  private static class SyncRowPending {
    final SyncRow syncRow;
    final boolean getOnly;
    final boolean shouldDeleteFiles;
    final boolean updateState;

    SyncRowPending(SyncRow syncRow, boolean getOnly, boolean shouldDeleteFiles, boolean updateState) {
      this.syncRow = syncRow;
      this.getOnly = getOnly;
      this.shouldDeleteFiles = shouldDeleteFiles;
      this.updateState = updateState;
    }
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
  private void synchronizeTableDataRowsAndAttachments(TableDefinitionEntry te,
      ArrayList<ColumnDefinition> orderedColumns, String displayName,
      boolean deferInstanceAttachments) {
    boolean success = true;
    boolean instanceFileSuccess = true;

    ArrayList<ColumnDefinition> fileAttachmentColumns = new ArrayList<ColumnDefinition>();
    for (ColumnDefinition cd : orderedColumns) {
      if (cd.getType().getDataType() == ElementDataType.rowpath) {
        fileAttachmentColumns.add(cd);
      }
    }

    log.i(TAG, "synchronizeTableDataRowsAndAttachments - deferInstanceAttachments: " + Boolean.toString(deferInstanceAttachments));

    // Prepare the tableResult. We'll start it as failure, and only update it
    // if we're successful at the end.
    String tableId = te.getTableId();
    TableResult tableResult = mUserResult.getTableResult(tableId);
    tableResult.setTableDisplayName(displayName);
    if (tableResult.getStatus() != Status.WORKING) {
      // there was some sort of error...
      log.e(TAG, "Skipping data sync - error in table schema or file verification step " + tableId);
      return;
    }

    if (tableId.equals("framework")) {
      // do not sync the framework table
      tableResult.setStatus(Status.SUCCESS);
      this.updateNotification(SyncProgressState.ROWS, R.string.table_data_sync_complete,
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

            this.updateNotification(SyncProgressState.ROWS,
                R.string.verifying_table_schema_on_server, new Object[] { tableId }, 0.0, false);

            // confirm that the local schema matches the one on the server.
            TableResource resource = synchronizer.getTableOrNull(tableId);

            if (resource == null) {
              // server does not know about it -- report that
              success = false;
              tableResult.setServerHadSchemaChanges(true);
              tableResult.setMessage("Server no longer has table! Reset App Server to upload.");
              tableResult.setStatus(Status.TABLE_DOES_NOT_EXIST_ON_SERVER);
              return;
            }

            // test that the schemaETag matches
            // if it doesn't, the user MUST sync app-level files and
            // configuration
            // syncing at the app level will adjust/set the local table
            // properties
            // schemaETag to match that on the server.
            String schemaETag = te.getSchemaETag();
            if (schemaETag == null || !resource.getSchemaETag().equals(schemaETag)) {
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

            this.updateNotification(SyncProgressState.ROWS,
                R.string.getting_changed_rows_on_server, new Object[] { tableId }, 5.0, false);

            IncomingRowModifications modification;
            try {
              modification = synchronizer.getUpdates(tableId, schemaETag, te.getLastDataETag());
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

            this.updateNotification(SyncProgressState.ROWS, R.string.anaylzing_row_changes,
                new Object[] { tableId }, 7.0, false);

            /**************************
             * PART 2: UPDATE THE DATA
             **************************/
            log.d(TAG, "updateDbFromServer setServerHadDataChanges(true)");
            tableResult.setServerHadDataChanges(modification.hasTableDataChanged());

            Map<String, SyncRow> changedServerRows = modification.getRows();

            // get all the rows in the data table -- we will iterate through
            // them all.
            UserTable localDataTable;
            {
              SQLiteDatabase db = null;

              try {
                db = DatabaseFactory.get().getDatabase(context, appName);
                localDataTable = ODKDatabaseUtils.get().rawSqlQuery(db, appName, tableId,
                    orderedColumns, null, null, null, null, DataTableColumns.ID, "ASC");
              } finally {
                if (db != null) {
                  db.close();
                  db = null;
                }
              }
            }

            containsConflicts = localDataTable.hasConflictRows();

            if (localDataTable.hasCheckpointRows()) {
              tableResult.setMessage(context.getString(R.string.table_contains_checkpoints));
              tableResult.setStatus(Status.TABLE_CONTAINS_CHECKPOINTS);
              return;
            }
            // these are all the various actions we will need to take:

            // serverRow updated; no matching localRow
            List<FileSyncRow> rowsToInsertLocally = new ArrayList<FileSyncRow>();

            // serverRow updated; localRow SyncState is synced or
            // synced_pending_files
            List<FileSyncRow> rowsToUpdateLocally = new ArrayList<FileSyncRow>();

            // serverRow deleted; localRow SyncState is synced or
            // synced_pending_files
            List<FileSyncRow> rowsToDeleteLocally = new ArrayList<FileSyncRow>();

            // serverRow updated or deleted; localRow SyncState is not synced or
            // synced_pending_files
            List<FileSyncRow> rowsToMoveToInConflictLocally = new ArrayList<FileSyncRow>();

            // localRow SyncState.new_row no changes pulled from server
            List<SyncRow> rowsToInsertOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.changed no changes pulled from server
            List<SyncRow> rowsToUpdateOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.deleted no changes pulled from server
            List<SyncRow> rowsToDeleteOnServer = new ArrayList<SyncRow>();

            // localRow SyncState.synced_pending_files no changes pulled from
            // server
            List<SyncRowPending> rowsToPushFileAttachments = new ArrayList<SyncRowPending>();

            // loop through the localRow table
            for (int i = 0; i < localDataTable.getNumberOfRows(); i++) {
              Row localRow = localDataTable.getRowAtIndex(i);
              String stateStr = localRow
                  .getRawDataOrMetadataByElementKey(DataTableColumns.SYNC_STATE);
              SyncState state = stateStr == null ? null : SyncState.valueOf(stateStr);

              String rowId = localRow.getRowId();

              // see if there is a change to this row that we need to pull down
              // from the server.
              SyncRow serverRow = changedServerRows.get(rowId);

              if (serverRow == null) {
                // the local row wasn't impacted by a server change
                // see if this local row should be pushed to the server.
                if (state == SyncState.new_row) {
                  rowsToInsertOnServer.add(convertToSyncRow(orderedColumns, localRow));
                } else if (state == SyncState.changed) {
                  rowsToUpdateOnServer.add(convertToSyncRow(orderedColumns, localRow));
                } else if (state == SyncState.deleted) {
                  rowsToDeleteOnServer.add(convertToSyncRow(orderedColumns, localRow));
                } else if (state == SyncState.synced_pending_files) {
                  rowsToPushFileAttachments.add(new SyncRowPending(convertToSyncRow(orderedColumns,
                      localRow), false, true, true));
                }
                // otherwise, it is in the synced state or in_conflict state
                // and nothing should be done with it...
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
                  rowsToDeleteLocally.add(new FileSyncRow(serverRow, convertToSyncRow(
                      orderedColumns, localRow), (state == SyncState.synced_pending_files)));
                } else {
                  rowsToUpdateLocally.add(new FileSyncRow(serverRow, convertToSyncRow(
                      orderedColumns, localRow), (state == SyncState.synced_pending_files)));
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
                rowsToDeleteLocally.add(new FileSyncRow(serverRow, convertToSyncRow(orderedColumns,
                    localRow), false));
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
                FileSyncRow syncRow = new FileSyncRow(serverRow, convertToSyncRow(orderedColumns,
                    localRow), false, localRowConflictType);

                if (!syncRow.identicalValues(orderedColumns)) {
                  if (syncRow.identicalValuesExceptRowETagAndFilterScope(orderedColumns)) {
                    // just apply the server RowETag and filterScope to the
                    // local row
                    rowsToUpdateLocally.add(new FileSyncRow(serverRow, convertToSyncRow(
                        orderedColumns, localRow), true));
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
                rowsToInsertLocally.add(new FileSyncRow(serverRow, null, false));
              }
            }

            //
            // OK we have captured the local inserting, locally updating,
            // locally deleting and conflicting actions. And we know
            // the changes for the server. Determine the per-row percentage
            // for applying all these changes

            int totalChange = rowsToInsertLocally.size() + rowsToUpdateLocally.size()
                + rowsToDeleteLocally.size() + rowsToMoveToInConflictLocally.size()
                + rowsToInsertOnServer.size() + rowsToUpdateOnServer.size()
                + rowsToDeleteOnServer.size() + rowsToPushFileAttachments.size();

            containsConflicts = containsConflicts || !rowsToMoveToInConflictLocally.isEmpty();

            perRowIncrement = 90.0 / ((double) (totalChange + 1));
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
                db = DatabaseFactory.get().getDatabase(context, appName);
                db.beginTransaction();
                success = deleteRowsInDb(db, resource, tableId, rowsToDeleteLocally,
                    fileAttachmentColumns, deferInstanceAttachments, tableResult);

                insertRowsInDb(db, resource, tableId, orderedColumns, rowsToInsertLocally,
                    rowsToPushFileAttachments, hasAttachments, tableResult);

                updateRowsInDb(db, resource, tableId, orderedColumns, rowsToUpdateLocally,
                    rowsToPushFileAttachments, hasAttachments, tableResult);

                conflictRowsInDb(db, resource, tableId, orderedColumns,
                    rowsToMoveToInConflictLocally, rowsToPushFileAttachments, hasAttachments, tableResult);

                // If we made it here and there was data, then we successfully
                // updated the data from the server.
                if (changedServerRows.size() > 0) {
                  tableResult.setPulledServerData(success);
                }

                // TODO: fix this for synced_pending_files
                // We likely need to relax this constraint on the
                // server?

                // We have to set the syncTag here so that the server
                // knows we saw its changes. Otherwise it won't let us
                // put up new information.
                if (success) {
                  // TODO: need to handle larger sets of results
                  // TODO: This is INCORRECT if we have a cursor continuation!!!
                  ODKDatabaseUtils.get().updateDBTableETags(db, tableId, resource.getSchemaETag(),
                      resource.getDataETag());
                  te.setSchemaETag(resource.getSchemaETag());
                  te.setLastDataETag(resource.getDataETag());
                }
                db.setTransactionSuccessful();
              } finally {
                if (db != null) {
                  db.endTransaction();
                  db.close();
                  db = null;
                }
              }
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
                log.e(TAG, "synchronizeTableSynced hadLocalDataChanges() returned "
                    + "true, and we're about to set it to true again. Odd.");
              }
              tableResult.setHadLocalDataChanges(true);
            }

            // push the changes up to the server
            boolean serverSuccess = false;
            try {

              // idempotent interface means that the interactions
              // for inserts, updates and deletes are identical.
              int count = 0;
              List<SyncRow> allAlteredRows = new ArrayList<SyncRow>();
              allAlteredRows.addAll(rowsToInsertOnServer);
              allAlteredRows.addAll(rowsToUpdateOnServer);
              allAlteredRows.addAll(rowsToDeleteOnServer);

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
                  RowOutcomeList outcomes = synchronizer.alterRows(tableId, te.getSchemaETag(),
                      te.getLastDataETag(), segmentAlter);

                  if (outcomes.getRows().size() != segmentAlter.size()) {
                    throw new IllegalStateException("Unexpected partial return?");
                  }

                  // process outcomes...
                  count = processRowOutcomes(te, resource, tableResult, orderedColumns,
                      rowsToPushFileAttachments, hasAttachments, 
                      count, allAlteredRows.size(), segmentAlter,
                      outcomes.getRows(), specialCases);

                  // process next segment...
                  offset = max;
                }
              }

              serverSuccess = specialCases.isEmpty();

              // And try to push the file attachments...
              count = 0;
              for (SyncRowPending syncRowPending : rowsToPushFileAttachments) {
                boolean outcome = true;
                if (!syncRowPending.getOnly) {
                  outcome = synchronizer.putFileAttachments(resource.getInstanceFilesUri(),
                      tableId, syncRowPending.syncRow, fileAttachmentColumns,
                      deferInstanceAttachments);
                }
                if (outcome) {
                  outcome = synchronizer.getFileAttachments(resource.getInstanceFilesUri(),
                      tableId, syncRowPending.syncRow, fileAttachmentColumns,
                      deferInstanceAttachments, syncRowPending.shouldDeleteFiles);
                  
                  if (syncRowPending.updateState) {
                    if (outcome) {
                      // OK -- we succeeded in putting/getting all attachments
                      // update our state to the synced state. 
                      SQLiteDatabase db = null;

                      try {
                        db = DatabaseFactory.get().getDatabase(context, appName);
                        ODKDatabaseUtils.get().updateRowETagAndSyncState(db, tableId,
                            syncRowPending.syncRow.getRowId(), syncRowPending.syncRow.getRowETag(),
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
                  this.updateNotification(SyncProgressState.ROWS,
                      R.string.uploading_attachments_server_row, new Object[] { tableId, count,
                          rowsToPushFileAttachments.size() }, 10.0 + rowsProcessed
                          * perRowIncrement, false);
                }
              }

              // And now update that we've pushed our changes to the server.
              tableResult.setPushedLocalData(true);
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
          db = DatabaseFactory.get().getDatabase(context, appName);
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
            this.updateNotification(SyncProgressState.ROWS,
                R.string.table_data_sync_with_conflicts, new Object[] { tableId }, 100.0, false);
          } else if (!instanceFileSuccess) {
            tableResult.setStatus(Status.TABLE_PENDING_ATTACHMENTS);
            this.updateNotification(SyncProgressState.ROWS,
                R.string.table_data_sync_pending_attachments, new Object[] { tableId }, 100.0,
                false);
          } else {
            tableResult.setStatus(Status.SUCCESS);
            this.updateNotification(SyncProgressState.ROWS, R.string.table_data_sync_complete,
                new Object[] { tableId }, 100.0, false);
          }
        }
      }
    }
  }

  private int processRowOutcomes(TableDefinitionEntry te, TableResource resource,
      TableResult tableResult, ArrayList<ColumnDefinition> orderedColumns,
      List<SyncRowPending> rowsToPushFileAttachments, boolean hasAttachments, 
      int countSoFar, int totalOutcomesSize,
      List<SyncRow> segmentAlter, ArrayList<RowOutcome> outcomes, ArrayList<RowOutcome> specialCases) {

    ArrayList<FileSyncRow> rowsToMoveToInConflictLocally = new ArrayList<FileSyncRow>();
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
      db = DatabaseFactory.get().getDatabase(context, appName);
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
            ODKDatabaseUtils.get().deleteDataInExistingDBTableWithId(db, appName,
                resource.getTableId(), r.getRowId());
            tableResult.incServerDeletes();
          } else {
            ODKDatabaseUtils.get().updateRowETagAndSyncState(db, resource.getTableId(),
                r.getRowId(), r.getRowETag(), 
                hasAttachments ? SyncState.synced_pending_files : SyncState.synced);
            // !!Important!! update the rowETag in our copy of this row.
            syncRow.setRowETag(r.getRowETag());
            if ( hasAttachments ) {
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
          SyncRow serverRow = new SyncRow(r.getRowId(), r.getRowETag(), r.isDeleted(), r.getFormId(),
              r.getLocale(), r.getSavepointType(), r.getSavepointTimestamp(),
              r.getSavepointCreator(), r.getFilterScope(), r.getValues());
          FileSyncRow conflictRow = new FileSyncRow(serverRow, syncRow, false, localRowConflictType);
  
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
          this.updateNotification(SyncProgressState.ROWS, R.string.altering_server_row, new Object[] {
              resource.getTableId(), countSoFar, totalOutcomesSize }, 10.0 + rowsProcessed
              * perRowIncrement, false);
        }
      }
      
      // process the conflict rows, if any
      conflictRowsInDb(db, resource, resource.getTableId(), orderedColumns,
          rowsToMoveToInConflictLocally, rowsToPushFileAttachments, hasAttachments, tableResult);

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
        db.close();
        db = null;
      }
    }
    
    return countSoFar;
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

    boolean identicalValuesExceptRowETagAndFilterScope(ArrayList<ColumnDefinition> orderedDefns) {
      if ((serverRow.getSavepointTimestamp() == null) ? (localRow.getSavepointTimestamp() != null)
          : !serverRow.getSavepointTimestamp().equals(localRow.getSavepointTimestamp())) {
        return false;
      }
      if ((serverRow.getSavepointCreator() == null) ? (localRow.getSavepointCreator() != null)
          : !serverRow.getSavepointCreator().equals(localRow.getSavepointCreator())) {
        return false;
      }
      if ((serverRow.getFormId() == null) ? (localRow.getFormId() != null) : !serverRow.getFormId()
          .equals(localRow.getFormId())) {
        return false;
      }
      if ((serverRow.getLocale() == null) ? (localRow.getLocale() != null) : !serverRow.getLocale()
          .equals(localRow.getLocale())) {
        return false;
      }
      if ((serverRow.getRowId() == null) ? (localRow.getRowId() != null) : !serverRow.getRowId()
          .equals(localRow.getRowId())) {
        return false;
      }
      if ((serverRow.getSavepointType() == null) ? (localRow.getSavepointType() != null)
          : !serverRow.getSavepointType().equals(localRow.getSavepointType())) {
        return false;
      }
      ArrayList<DataKeyValue> localValues = localRow.getValues();
      ArrayList<DataKeyValue> serverValues = serverRow.getValues();

      if (localValues == null && serverValues == null) {
        return true;
      } else if (localValues == null || serverValues == null) {
        return false;
      }

      if (localValues.size() != serverValues.size()) {
        return false;
      }

      for (int i = 0; i < localValues.size(); ++i) {
        DataKeyValue local = localValues.get(i);
        DataKeyValue server = serverValues.get(i);
        if (!local.column.equals(server.column)) {
          return false;
        }
        if (local.value == null && server.value == null) {
          continue;
        } else if (local.value == null || server.value == null) {
          return false;
        } else if (local.value.equals(server.value)) {
          continue;
        }

        // NOT textually identical.
        //
        // Everything must be textually identical except possibly number fields
        // which may have rounding due to different database implementations,
        // data representations, and marshaling libraries.
        //
        ColumnDefinition cd = ColumnDefinition.find(orderedDefns, local.column);
        if (cd.getType().getDataType() == ElementDataType.number) {
          // !!Important!! Double.valueOf(str) handles NaN and +/-Infinity
          Double localNumber = Double.valueOf(local.value);
          Double serverNumber = Double.valueOf(server.value);

          if (localNumber.equals(serverNumber)) {
            // simple case -- trailing zeros or string representation mix-up
            //
            continue;
          } else if (localNumber.isInfinite() && serverNumber.isInfinite()) {
            // if they are both plus or both minus infinity, we have a match
            if (Math.signum(localNumber) == Math.signum(serverNumber)) {
              continue;
            } else {
              return false;
            }
          } else if (localNumber.isNaN() || localNumber.isInfinite() || serverNumber.isNaN()
              || serverNumber.isInfinite()) {
            // one or the other is special1
            return false;
          } else {
            double localDbl = localNumber;
            double serverDbl = serverNumber;
            if (localDbl == serverDbl) {
              continue;
            }
            // OK. We have two values like 9.80 and 9.8
            // consider them equal if they are adjacent to each other.
            double localNear = localDbl;
            int idist = 0;
            int idistMax = 128;
            for (idist = 0; idist < idistMax; ++idist) {
              localNear = Math.nextAfter(localNear, serverDbl);
              if (localNear == serverDbl) {
                break;
              }
            }
            if (idist < idistMax) {
              continue;
            }
            return false;
          }
        } else {
          // textual identity is required!
          return false;
        }
      }
      if (!localValues.containsAll(serverValues)) {
        return false;
      }
      return true;
    }

    boolean identicalValues(ArrayList<ColumnDefinition> orderedDefns) {
      if ((serverRow.getFilterScope() == null) ? (localRow.getFilterScope() != null) : !serverRow
          .getFilterScope().equals(localRow.getFilterScope())) {
        return false;
      }
      if ((serverRow.getRowETag() == null) ? (localRow.getRowETag() != null) : !serverRow
          .getRowETag().equals(localRow.getRowETag())) {
        return false;
      }
      return identicalValuesExceptRowETagAndFilterScope(orderedDefns);
    }
  };

  private void clientWebException(String method, String tableId, ClientWebException e,
      TableResult tableResult) {
    log.e(TAG, String.format("ResourceAccessException in %s for table: %s exception: %s", method,
        tableId, e.toString()));
    tableResult.setStatus(Status.AUTH_EXCEPTION);
    tableResult.setMessage(e.getMessage());
  }

  private void ioException(String method, String tableId, IOException e, TableResult tableResult) {
    log.e(
        TAG,
        String.format("IOException in %s for table: %s exception: %s", method, tableId,
            e.toString()));
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(e.getMessage());
  }

  private void exception(String method, String tableId, Exception e, TableResult tableResult) {
    log.e(
        TAG,
        String.format("Unexpected exception in %s on table: %s exception: %s", method, tableId,
            e.toString()));
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(e.getMessage());
  }

  private void conflictRowsInDb(SQLiteDatabase db, TableResource resource, String tableId,
      ArrayList<ColumnDefinition> orderedColumns, List<FileSyncRow> changes,
      List<SyncRowPending> rowsToPushFileAttachments, boolean hasAttachments, TableResult tableResult)
      throws ClientWebException {

    int count = 0;
    for (FileSyncRow change : changes) {
      SyncRow serverRow = change.serverRow;
      log.i(TAG,
          "conflicting row, id=" + serverRow.getRowId() + " rowETag=" + serverRow.getRowETag());
      ContentValues values = new ContentValues();

      // delete the old server-values in_conflict row if it exists
      ODKDatabaseUtils.get().deleteServerConflictRowWithId(db, tableId, serverRow.getRowId());
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
        ODKDatabaseUtils.get().updateRowETagAndSyncState(db, tableId, serverRow.getRowId(), null,
            SyncState.new_row);
        // and physically delete it.
        ODKDatabaseUtils.get().deleteDataInExistingDBTableWithId(db, appName, tableId,
            serverRow.getRowId());

        tableResult.incLocalDeletes();
      } else {
        // update the localRow to be in_conflict
        ODKDatabaseUtils.get().placeRowIntoConflict(db, tableId, serverRow.getRowId(),
            localRowConflictType);

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
        ODKDatabaseUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
            values, serverRow.getRowId());

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
        if ( hasAttachments ) {
          rowsToPushFileAttachments.add(new SyncRowPending(change.localRow, true, false, false));
          rowsToPushFileAttachments.add(new SyncRowPending(serverRow, true, false, false));
        }
      }
      ++count;
      ++rowsProcessed;
      if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
        this.updateNotification(SyncProgressState.ROWS, R.string.marking_conflicting_local_row,
            new Object[] { tableId, count, changes.size() },
            10.0 + rowsProcessed * perRowIncrement, false);
      }
    }
  }

  private void insertRowsInDb(SQLiteDatabase db, TableResource resource, String tableId,
      ArrayList<ColumnDefinition> orderedColumns, List<FileSyncRow> changes,
      List<SyncRowPending> rowsToPushFileAttachments, boolean hasAttachments, TableResult tableResult)
      throws ClientWebException {
    int count = 0;
    for (FileSyncRow change : changes) {
      SyncRow serverRow = change.serverRow;
      ContentValues values = new ContentValues();

      values.put(DataTableColumns.ID, serverRow.getRowId());
      values.put(DataTableColumns.ROW_ETAG, serverRow.getRowETag());
      values.put(DataTableColumns.SYNC_STATE, hasAttachments ? SyncState.synced_pending_files.name() : SyncState.synced.name());
      values.put(DataTableColumns.FORM_ID, serverRow.getFormId());
      values.put(DataTableColumns.LOCALE, serverRow.getLocale());
      values.put(DataTableColumns.SAVEPOINT_TIMESTAMP, serverRow.getSavepointTimestamp());
      values.put(DataTableColumns.SAVEPOINT_CREATOR, serverRow.getSavepointCreator());

      for (DataKeyValue entry : serverRow.getValues()) {
        String colName = entry.column;
        values.put(colName, entry.value);
      }

      ODKDatabaseUtils.get().insertDataIntoExistingDBTableWithId(db, tableId, orderedColumns,
          values, serverRow.getRowId());
      tableResult.incLocalInserts();

      if ( hasAttachments ) {
        rowsToPushFileAttachments.add(new SyncRowPending(serverRow, true, true, true));
      }
      ++count;
      ++rowsProcessed;
      if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
        this.updateNotification(SyncProgressState.ROWS, R.string.inserting_local_row, new Object[] {
            tableId, count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
      }
    }
  }

  private void updateRowsInDb(SQLiteDatabase db, TableResource resource, String tableId,
      ArrayList<ColumnDefinition> orderedColumns, List<FileSyncRow> changes,
      List<SyncRowPending> rowsToPushFileAttachments, boolean hasAttachments, TableResult tableResult)
      throws ClientWebException {
    int count = 0;
    for (FileSyncRow change : changes) {
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
      values.put(DataTableColumns.SYNC_STATE, hasAttachments ? SyncState.synced_pending_files.name() : SyncState.synced.name());
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

      ODKDatabaseUtils.get().updateDataInExistingDBTableWithId(db, tableId, orderedColumns, values,
          serverRow.getRowId());
      tableResult.incLocalUpdates();

      if ( hasAttachments ) {
        rowsToPushFileAttachments.add(new SyncRowPending(serverRow, false, true, true));
      }

      ++count;
      ++rowsProcessed;
      if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
        this.updateNotification(SyncProgressState.ROWS, R.string.updating_local_row, new Object[] {
            tableId, count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
      }
    }
  }

  private boolean deleteRowsInDb(SQLiteDatabase db, TableResource resource, String tableId,
      List<FileSyncRow> changes, ArrayList<ColumnDefinition> fileAttachmentColumns,
      boolean deferInstanceAttachments, TableResult tableResult) throws IOException {
    int count = 0;
    boolean deletesAllSuccessful = true;
    for (FileSyncRow change : changes) {
      if (change.isRestPendingFiles) {
        boolean outcome = synchronizer.putFileAttachments(resource.getInstanceFilesUri(), tableId,
            change.localRow, fileAttachmentColumns, deferInstanceAttachments);
        if (outcome) {
          // move the local record into the 'new_row' sync state
          // so it can be physically deleted.
          ODKDatabaseUtils.get().updateRowETagAndSyncState(db, tableId,
              change.serverRow.getRowId(), null, SyncState.new_row);
          // and physically delete it.
          ODKDatabaseUtils.get().deleteDataInExistingDBTableWithId(db, appName, tableId,
              change.serverRow.getRowId());

          tableResult.incLocalDeletes();
        } else {
          deletesAllSuccessful = false;
        }
      }
      ++count;
      ++rowsProcessed;
      if (rowsProcessed % ROWS_BETWEEN_PROGRESS_UPDATES == 0) {
        this.updateNotification(SyncProgressState.ROWS, R.string.deleting_local_row, new Object[] {
            tableId, count, changes.size() }, 10.0 + rowsProcessed * perRowIncrement, false);
      }
    }
    return deletesAllSuccessful;
  }

  private SyncRow convertToSyncRow(ArrayList<ColumnDefinition> orderedColumns, Row localRow) {
    String rowId = localRow.getRowId();
    String rowETag = localRow.getRawDataOrMetadataByElementKey(DataTableColumns.ROW_ETAG);
    ArrayList<DataKeyValue> values = new ArrayList<DataKeyValue>();

    for (ColumnDefinition column : orderedColumns) {
      if (column.isUnitOfRetention()) {
        String elementKey = column.getElementKey();
        values.add(new DataKeyValue(elementKey, localRow
            .getRawDataOrMetadataByElementKey(elementKey)));
      }
    }
    SyncRow syncRow = new SyncRow(rowId, rowETag, false,
        localRow.getRawDataOrMetadataByElementKey(DataTableColumns.FORM_ID),
        localRow.getRawDataOrMetadataByElementKey(DataTableColumns.LOCALE),
        localRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TYPE),
        localRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_TIMESTAMP),
        localRow.getRawDataOrMetadataByElementKey(DataTableColumns.SAVEPOINT_CREATOR),
        Scope.asScope(localRow.getRawDataOrMetadataByElementKey(DataTableColumns.FILTER_TYPE),
            localRow.getRawDataOrMetadataByElementKey(DataTableColumns.FILTER_VALUE)), values);
    return syncRow;
  }

  /**
   * Update the database to reflect the new structure.
   * <p>
   * This should be called when downloading a table from the server, which is
   * why the syncTag is separate.
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
  private ArrayList<ColumnDefinition> addTableFromDefinitionResource(SQLiteDatabase db,
      TableDefinitionResource definitionResource, boolean doesNotExistLocally)
      throws JsonParseException, JsonMappingException, IOException, SchemaMismatchException {
    if (doesNotExistLocally) {
      try {
        ArrayList<ColumnDefinition> orderedDefns;
        db.beginTransaction();
        orderedDefns = ODKDatabaseUtils.get().createOrOpenDBTableWithColumns(db, appName,
            definitionResource.getTableId(), definitionResource.getColumns());
        ODKDatabaseUtils.get().updateDBTableETags(db, definitionResource.getTableId(),
            definitionResource.getSchemaETag(), null);
        db.setTransactionSuccessful();
        return orderedDefns;
      } finally {
        db.endTransaction();
      }
    } else {
      List<Column> localColumns = ODKDatabaseUtils.get().getUserDefinedColumns(db,
          definitionResource.getTableId());
      List<Column> serverColumns = definitionResource.getColumns();

      if (localColumns.size() != serverColumns.size()) {
        throw new SchemaMismatchException("Server schema differs from local schema");
      }

      for (int i = 0; i < serverColumns.size(); ++i) {
        Column server = serverColumns.get(i);
        Column local = localColumns.get(i);
        if (!local.equals(server)) {
          throw new SchemaMismatchException("Server schema differs from local schema");
        }
      }

      TableDefinitionEntry te = ODKDatabaseUtils.get().getTableDefinitionEntry(db,
          definitionResource.getTableId());
      String schemaETag = te.getSchemaETag();
      if (schemaETag == null || !schemaETag.equals(definitionResource.getSchemaETag())) {
        // server has changed its schema
        try {
          db.beginTransaction();
          // change row sync and conflict status to handle new server schema.
          // Clean up this table and set the dataETag to null.
          ODKDatabaseUtils.get().changeDataRowsToNewRowState(db, definitionResource.getTableId());
          // and update to the new schemaETag, but clear our dataETag
          // so that all data rows sync.
          ODKDatabaseUtils.get().updateDBTableETags(db, definitionResource.getTableId(),
              definitionResource.getSchemaETag(), null);
          db.setTransactionSuccessful();
        } finally {
          db.endTransaction();
        }
      }
      return ColumnDefinition.buildColumnDefinitions(appName, definitionResource.getTableId(),
          localColumns);
    }
  }
}
