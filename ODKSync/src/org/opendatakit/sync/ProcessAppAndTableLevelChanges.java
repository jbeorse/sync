package org.opendatakit.sync;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.http.HttpStatus;
import org.apache.wink.client.ClientWebException;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinitionResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResourceList;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.common.android.data.TableDefinitionEntry;
import org.opendatakit.common.android.utilities.ODKDatabaseUtils;
import org.opendatakit.common.android.utilities.SyncETagsUtils;
import org.opendatakit.common.android.utilities.TableUtil;
import org.opendatakit.common.android.utilities.WebLogger;
import org.opendatakit.sync.SynchronizationResult.Status;
import org.opendatakit.sync.Synchronizer.OnTablePropertiesChanged;
import org.opendatakit.sync.application.Sync;
import org.opendatakit.sync.exceptions.InvalidAuthTokenException;
import org.opendatakit.sync.exceptions.SchemaMismatchException;
import org.opendatakit.sync.service.SyncProgressState;

import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;

/**
 * Isolate the app-level and table-level synchronization steps
 * into this class, reducing the size of the original SyncProcessor.
 * 
 * @author mitchellsundt@gmail.com
 *
 */
public class ProcessAppAndTableLevelChanges {

  private static final String TAG = ProcessAppAndTableLevelChanges.class.getSimpleName();

  private WebLogger log;
  
  private SyncExecutionContext sc;

  public ProcessAppAndTableLevelChanges(SyncExecutionContext sc) {
    this.sc = sc;
    this.log = WebLogger.getLogger(sc.getAppName());
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
  public List<TableResource> synchronizeConfigurationAndContent(boolean pushToServer) {
    log.i(TAG, "entered synchronizeConfigurationAndContent()");

    boolean issueDeletes = false;
    if (Sync.getInstance().shouldWaitForDebugger()) {
      issueDeletes = true;
      android.os.Debug.waitForDebugger();
    }

    sc.updateNotification(SyncProgressState.STARTING, R.string.retrieving_tables_list_from_server, null, 0.0, false);

    // working list of tables -- the list we will construct and return...
    List<TableResource> workingListOfTables = new ArrayList<TableResource>();

    // get tables from server
    TableResourceList tableList;
    List<TableResource> tables = new ArrayList<TableResource>();
    // For now, repeatedly do this until we get all of the tables on the server.
    // This will likely need to change if we actually have 1000's of them...
    String webSafeResumeCursor = null;
    for (;;) {
      try {
        tableList = sc.getSynchronizer().getTables(webSafeResumeCursor);
        if (tableList != null & tableList.getTables() != null) {
          tables.addAll(tableList.getTables());
        }
      } catch (InvalidAuthTokenException e) {
        sc.setAppLevelStatus(Status.AUTH_EXCEPTION);
        log.i(TAG,
            "[synchronizeConfigurationAndContent] Could not retrieve server table list exception: "
                + e.toString());
        return new ArrayList<TableResource>();
      } catch (ClientWebException e) {
        if ( e.getResponse() != null && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED ) {
          sc.setAppLevelStatus(Status.AUTH_EXCEPTION);
        } else {
          sc.setAppLevelStatus(Status.EXCEPTION);
        }
        log.i(TAG,
            "[synchronizeConfigurationAndContent] Could not retrieve server table list exception: "
                + e.toString());
        return new ArrayList<TableResource>();
      } catch (Exception e) {
        sc.setAppLevelStatus(Status.EXCEPTION);
        log.e(TAG,
            "[synchronizeConfigurationAndContent] Unexpected exception getting server table list exception: "
                + e.toString());
        return new ArrayList<TableResource>();
      }
      if ( !tableList.isHasMoreResults() ) {
        break;
      }
      webSafeResumeCursor = tableList.getWebSafeResumeCursor();
    }

    // TODO: do the database updates with a few database transactions...
    // get the tables on the local device
    List<String> localTableIds;
    SQLiteDatabase db = null;
    try {
      db = sc.getDatabase();
      localTableIds = ODKDatabaseUtils.get().getAllTableIds(db);
    } catch (SQLiteException e) {
      sc.setAppLevelStatus(Status.EXCEPTION);
      log.e(TAG,
          "[synchronizeConfigurationAndContent] Unexpected exception getting local tableId list exception: "
              + e.toString());
      return new ArrayList<TableResource>();
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
      int nMajorSyncSteps = 1 + (pushToServer ? 2 * localTableIds.size()
          : (uniqueTableIds.size() + tables.size()));
      
      sc.resetMajorSyncSteps(nMajorSyncSteps);
    }

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
      boolean success = sc.getSynchronizer().syncAppLevelFiles(pushToServer,
          tableList.getAppLevelManifestETag(), sc);
      sc.setAppLevelStatus(success ? Status.SUCCESS : Status.FAILURE);
    } catch (InvalidAuthTokenException e) {
      // TODO: update a synchronization result to report back to them as well.
      sc.setAppLevelStatus(Status.AUTH_EXCEPTION);
      log.e(TAG,
          "[synchronizeConfigurationAndContent] auth token failure while trying to synchronize app-level files.");
      log.printStackTrace(e);
      return new ArrayList<TableResource>();
    } catch (ClientWebException e) {
      // TODO: update a synchronization result to report back to them as well.
      if ( e.getResponse() != null && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED ) {
        sc.setAppLevelStatus(Status.AUTH_EXCEPTION);
      } else {
        sc.setAppLevelStatus(Status.EXCEPTION);
      }
      log.e(TAG,
          "[synchronizeConfigurationAndContent] error trying to synchronize app-level files.");
      log.printStackTrace(e);
      return new ArrayList<TableResource>();
    }

    // done with app-level file synchronization
    sc.incMajorSyncStep();

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
            db = sc.getDatabase();
            entry = ODKDatabaseUtils.get().getTableDefinitionEntry(db, localTableId);
            orderedDefns = TableUtil.get().getColumnDefinitions(db, sc.getAppName(), localTableId);
          } finally {
            if (db != null) {
              db.close();
              db = null;
            }
          }

          if (matchingResource != null) {
            serverTablesToDelete.remove(matchingResource);
          }
          // do not sync the framework table
          TableResource updatedResource = synchronizeTableConfigurationAndContent(entry,
              orderedDefns, matchingResource, true);
          if (updatedResource != null) {
            // there were no errors sync'ing the table-level info.
            // allow client to sync instance-level data...
            workingListOfTables.add(updatedResource);
          }
        }

        sc.updateNotification(SyncProgressState.TABLE_FILES,
            R.string.table_level_file_sync_complete, new Object[] { localTableId }, 100.0, false);
        sc.incMajorSyncStep();
      }

      // TODO: make this configurable?
      // Generally should not allow this, as it is very dangerous
      // delete any other tables
      if (issueDeletes) {
        for (TableResource tableToDelete : serverTablesToDelete) {
          try {
            sc.getSynchronizer().deleteTable(tableToDelete);
          } catch (InvalidAuthTokenException e) {
            // TODO: update a synchronization result to report back to them as well.
            sc.setAppLevelStatus(Status.AUTH_EXCEPTION);
            log.e(TAG,
                "[synchronizeConfigurationAndContent] auth token failure while trying to delete tables.");
            log.printStackTrace(e);
            return new ArrayList<TableResource>();
          } catch (ClientWebException e) {
            // TODO: update a synchronization result to report back to them as well.
            if ( e.getResponse() != null && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED ) {
              sc.setAppLevelStatus(Status.AUTH_EXCEPTION);
            } else {
              sc.setAppLevelStatus(Status.EXCEPTION);
            }
            log.e(TAG,
                "[synchronizeConfigurationAndContent] error trying to delete tables.");
            log.printStackTrace(e);
            return new ArrayList<TableResource>();
          }
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

      boolean firstTime = true;
      for (TableResource table : tables) {
        if ( !firstTime ) {
          sc.incMajorSyncStep();
        }
        firstTime = false;

        ArrayList<ColumnDefinition> orderedDefns = null;

        String serverTableId = table.getTableId();

        TableResult tableResult = sc.getTableResult(serverTableId);

        boolean doesNotExistLocally = true;
        boolean isLocalMatch = false;
        TableDefinitionEntry entry = null;

        if (localTableIds.contains(serverTableId)) {
          localTableIdsToDelete.remove(serverTableId);
          doesNotExistLocally = false;

          // see if the schemaETag matches. If so, we can skip a lot of steps...
          // no need to verify schema match -- just sync files...
          try {
            db = sc.getDatabase();
            entry = ODKDatabaseUtils.get().getTableDefinitionEntry(db, serverTableId);
            orderedDefns = TableUtil.get().getColumnDefinitions(db, sc.getAppName(), serverTableId);
            if (table.getSchemaETag().equals(entry.getSchemaETag())) {
              isLocalMatch = true;
            }
          } catch (SQLiteException e) {
            exception("synchronizeConfigurationAndContent - database exception", serverTableId, e, tableResult);
            continue;
          } finally {
            if (db != null) {
              db.close();
              db = null;
            }
          }
        }

        sc.updateNotification(SyncProgressState.TABLE_FILES,
            (doesNotExistLocally ? R.string.creating_local_table
                : R.string.verifying_table_schema_on_server), new Object[] { serverTableId }, 0.0,
            false);

        if (!isLocalMatch) {
          try {
            TableDefinitionResource definitionResource = sc.getSynchronizer().getTableDefinition(table
                .getDefinitionUri());

            try {
              db = sc.getDatabase();
              orderedDefns = addTableFromDefinitionResource(db, definitionResource,
                  doesNotExistLocally);
              entry = ODKDatabaseUtils.get().getTableDefinitionEntry(db, serverTableId);
            } finally {
              if (db != null) {
                db.close();
                db = null;
              }
            }
          } catch (ClientWebException e) {
            if ( e.getResponse() != null && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED ) {
              clientAuthException("synchronizeConfigurationAndContent", serverTableId, e, tableResult);
            } else {
              clientWebException("synchronizeConfigurationAndContent - Unexpected exception parsing table definition exception",
                serverTableId, e, tableResult);
            }
            continue;
          } catch (InvalidAuthTokenException e) {
            clientAuthException("synchronizeConfigurationAndContent", serverTableId, e, tableResult);
            continue;
          } catch (SchemaMismatchException e) {
            exception("synchronizeConfigurationAndContent - schema for this table does not match that on the server",
                serverTableId, e, tableResult);
            continue;
          } catch (Exception e) {
            exception("synchronizeConfigurationAndContent - Unexpected exception accessing table definition",
                serverTableId, e, tableResult);
            continue;
          }
        }

        // Sync the local media files with the server if the table
        // existed locally before we attempted downloading it.

        TableResource updatedResource = synchronizeTableConfigurationAndContent(entry,
            orderedDefns, table, false);
        if (updatedResource != null) {
          // there were no errors sync'ing the table-level info.
          // allow client to sync instance-level data...
          workingListOfTables.add(updatedResource);
        }
        sc.updateNotification(SyncProgressState.TABLE_FILES,
            R.string.table_level_file_sync_complete, new Object[] { serverTableId }, 100.0, false);
      }
      sc.incMajorSyncStep();

      // and now loop through the ones to delete...
      for (String localTableId : localTableIdsToDelete) {
        sc.updateNotification(SyncProgressState.TABLE_FILES, R.string.dropping_local_table,
            new Object[] { localTableId }, 0.0, false);
        // eventually might not be true if there are multiple syncs running
        // simultaneously...
        TableResult tableResult = sc.getTableResult(localTableId);
        try {
          db = sc.getDatabase();
          ODKDatabaseUtils.get().deleteDBTableAndAllData(db, sc.getAppName(), localTableId);
          tableResult.setStatus(Status.SUCCESS);
        } catch (SQLiteException e) {
          exception("synchronizeConfigurationAndContent - database exception deleting table", localTableId, e, tableResult);
        } catch (Exception e) {
          exception("synchronizeConfigurationAndContent - unexpected exception deleting table", localTableId, e, tableResult);
        } finally {
          if (db != null) {
            db.close();
            db = null;
          }
        }
        sc.incMajorSyncStep();
      }
    }
    // be sure we sort them alphabetically...
    Collections.sort(workingListOfTables);
    return workingListOfTables;
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
   * @return null if there is an error, otherwise a new or updated table
   *         resource
   * @throws InvalidAuthTokenException 
   * @throws ClientWebException 
   */
  private TableResource synchronizeTableConfigurationAndContent(TableDefinitionEntry te,
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

    sc.updateNotification(SyncProgressState.TABLE_FILES,
        R.string.verifying_table_schema_on_server, new Object[] { tableId }, 0.0, false);
    final TableResult tableResult = sc.getTableResult(tableId);
    String displayName;
    SQLiteDatabase db = null;
    try {
      db = sc.getDatabase();
      displayName = TableUtil.get().getLocalizedDisplayName(db, tableId);
      tableResult.setTableDisplayName(displayName);
    } finally {
      if (db != null) {
        db.close();
        db = null;
      }
    }

    try {
      String schemaETag = te.getSchemaETag();

      if (resource == null) {
        // exists locally but not on server...

        if (!pushLocalTableLevelFiles) {
          throw new IllegalStateException("This code path should no longer be followed!");
        }

        // the insert of the table was incomplete -- try again

        // we are creating data on the server

        try {
          db = sc.getDatabase();
          db.beginTransaction();
          // change row sync and conflict status to handle new server schema.
          // Clean up this table and set the dataETag to null.
          ODKDatabaseUtils.get().changeDataRowsToNewRowState(db, tableId);
          // we need to clear out the dataETag so
          // that we will pull all server changes and sync our properties.
          ODKDatabaseUtils.get().updateDBTableETags(db, tableId, null, null);
          //
          // Although the server does not recognize this tableId, we can
          // keep our record of the ETags for the table-level files and
          // manifest. These may enable us to short-circuit the restoration
          // of the table-level files should another client be simultaneously
          // trying to restore those files to the server.
          //
          // However, we do need to delete all the instance-level files,
          // as these are tied to the schemaETag we hold, and that is now
          // invalid.
          if (schemaETag != null) {
            // if the local table ever had any server sync information for this
            // host then clear it. If the user changed the server URL, we have
            // already cleared this information.
            //
            // Clearing it here handles the case where an admin deleted the
            // table on the server and we are now re-pushing that table to
            // the server.
            SyncETagsUtils seu = new SyncETagsUtils();
            URI tableInstanceFilesUri = sc.getSynchronizer().constructTableInstanceFileUri(tableId,
                schemaETag);
            seu.deleteAllSyncETagsUnderServerUri(db, tableInstanceFilesUri);
          }

          db.setTransactionSuccessful();
        } finally {
          if (db != null) {
            db.endTransaction();
            db.close();
            db = null;
          }
        }

        /**************************
         * PART 1A: CREATE THE TABLE First we need to create the table on the
         * server. This comes in two parts--the definition and the properties.
         **************************/
        // First create the table definition on the server.
        try {
          resource = sc.getSynchronizer().createTable(tableId, schemaETag,
              ColumnDefinition.getColumns(orderedDefns));
        } catch (ClientWebException e) {
          if ( e.getResponse() != null && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED ) {
            clientAuthException("synchronizeTableConfigurationAndContent - createTable on server", tableId, e, tableResult);
          } else {
            clientWebException("synchronizeTableConfigurationAndContent - createTable on server", tableId, e, tableResult);
          }
          return null;
        } catch (InvalidAuthTokenException e) {
          clientAuthException("synchronizeTableConfigurationAndContent - createTable on server", tableId, e, tableResult);
          return null;
        } catch (Exception e) {
          exception("synchronizeTableConfigurationAndContent - createTable on server", tableId, e, tableResult);
          return null;
        }

        schemaETag = resource.getSchemaETag();
        try {
          db = sc.getDatabase();
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
          definitionResource = sc.getSynchronizer().getTableDefinition(resource.getDefinitionUri());
        } catch (ClientWebException e) {
          if ( e.getResponse() != null && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED ) {
            clientAuthException("synchronizeTableConfigurationAndContent - get table definition from server", tableId, e, tableResult);
          } else {
            clientWebException("synchronizeTableConfigurationAndContent - get table definition from server", tableId, e, tableResult);
          }
          return null;
        } catch (InvalidAuthTokenException e) {
          clientAuthException("synchronizeTableConfigurationAndContent - get table definition from server", tableId, e, tableResult);
          return null;
        } catch (Exception e) {
          exception("synchronizeTableConfigurationAndContent - get table definition from server", tableId, e, tableResult);
          return null;
        }

        // record that we have pulled it
        tableResult.setPulledServerSchema(true);
        try {
          db = sc.getDatabase();
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
          return null;
        } catch (Exception e) {
          exception("synchronizeTableConfigurationAndContent - create table locally", tableId, e, tableResult);
          return null;
        } finally {
          if (db != null) {
            db.close();
            db = null;
          }
        }
      }

      // OK. we have the schemaETag matching.

      // write our properties and definitions files.
      // write the current schema and properties set.
      try {
        db = sc.getDatabase();
        sc.getCsvUtil().writePropertiesCsv(db, tableId, orderedDefns);
      } finally {
        if (db != null) {
          db.close();
          db = null;
        }
      }

      try {
        sc.getSynchronizer().syncTableLevelFiles(tableId, resource.getTableLevelManifestETag(),
            new OnTablePropertiesChanged() {
              @Override
              public void onTablePropertiesChanged(String tableId) {
                try {
                  sc.getCsvUtil().updateTablePropertiesFromCsv(null, tableId);
                } catch (IOException e) {
                  log.printStackTrace(e);
                  String msg = e.getMessage();
                  if (msg == null)
                    msg = e.toString();
                  tableResult.setMessage(msg);
                  tableResult.setStatus(Status.EXCEPTION);
                }
              }
            }, pushLocalTableLevelFiles, sc);
      } catch (ClientWebException e) {
        if ( e.getResponse() != null && e.getResponse().getStatusCode() == HttpStatus.SC_UNAUTHORIZED ) {
          clientAuthException("synchronizeTableConfigurationAndContent", tableId, e, tableResult);
        } else {
          clientWebException("synchronizeTableConfigurationAndContent - Unexpected exception parsing table definition exception",
              tableId, e, tableResult);
        }
        return null;
      } catch (InvalidAuthTokenException e) {
        clientAuthException("synchronizeTableConfigurationAndContent", tableId, e, tableResult);
        return null;
      }

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

    // this should be non-null at this point...
    return resource;
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
   * @throws SchemaMismatchException
   */
  private ArrayList<ColumnDefinition> addTableFromDefinitionResource(SQLiteDatabase db,
      TableDefinitionResource definitionResource, boolean doesNotExistLocally)
      throws SchemaMismatchException {
    if (doesNotExistLocally) {
      try {
        ArrayList<ColumnDefinition> orderedDefns;
        db.beginTransaction();
        orderedDefns = ODKDatabaseUtils.get().createOrOpenDBTableWithColumns(db, sc.getAppName(),
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
      return ColumnDefinition.buildColumnDefinitions(sc.getAppName(), definitionResource.getTableId(),
          localColumns);
    }
  }

  /**
   * Common error reporting...
   * 
   * @param method
   * @param tableId
   * @param e
   * @param tableResult
   */
  private void clientAuthException(String method, String tableId, Exception e,
      TableResult tableResult) {
    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }
    log.printStackTrace(e);
    log.e(TAG, String.format("ResourceAccessException in %s for table: %s exception: %s", method,
        tableId, msg));
    tableResult.setStatus(Status.AUTH_EXCEPTION);
    tableResult.setMessage(msg);
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
    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }
    log.printStackTrace(e);
    log.e(TAG, String.format("ResourceAccessException in %s for table: %s exception: %s", method,
        tableId, msg));
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(msg);
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
    String msg = e.getMessage();
    if (msg == null) {
      msg = e.toString();
    }            
    log.printStackTrace(e);
    log.e(TAG, String.format("Unexpected exception in %s on table: %s exception: %s", method,
        tableId, msg));
    tableResult.setStatus(Status.EXCEPTION);
    tableResult.setMessage(msg);
  }

}
