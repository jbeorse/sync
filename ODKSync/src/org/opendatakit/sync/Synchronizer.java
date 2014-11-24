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

import java.util.ArrayList;
import java.util.List;

import org.apache.wink.client.ClientWebException;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.TableDefinitionResource;
import org.opendatakit.aggregate.odktables.rest.entity.TableResource;
import org.opendatakit.common.android.data.ColumnDefinition;
import org.opendatakit.sync.service.SyncProgressState;

/**
 * Synchronizer abstracts synchronization of tables to an external cloud/server.
 *
 * @author the.dylan.price@gmail.com
 * @author sudar.sam@gmail.com
 *
 */
public interface Synchronizer {

  public interface SynchronizerStatus {
    /**
     * Status of this action.
     *
     * @param text
     * @param progressPercentage
     *          0..100
     * @param indeterminateProgress
     *          true if progressGrains is N/A
     */
    void updateNotification(SyncProgressState state, int textResource, Object[] formatArgVals,
        Double progressPercentage, boolean indeterminateProgress);
  }

  public interface OnTablePropertiesChanged {
    void onTablePropertiesChanged(String tableId);
  }

  /**
   * Get a list of all tables in the server.
   *
   * @return a list of the table resources on the server
   * @throws ClientWebException
   */
  public List<TableResource> getTables() throws ClientWebException;

  /**
   * Discover the current sync state of a given tableId. This may throw an
   * exception if the table is not found on the server.
   *
   * @param tableId
   * @return
   * @throws ClientWebException
   */
  public TableResource getTable(String tableId) throws ClientWebException;

  /**
   * Returns the given tableId resource or null if the resource does not exist
   * on the server.
   *
   * @param tableId
   * @return
   * @throws ClientWebException
   */
  public TableResource getTableOrNull(String tableId) throws ClientWebException;

  /**
   * Discover the schema for a table resource.
   *
   * @param tableDefinitionUri
   * @return the table definition
   * @throws ClientWebException
   */
  public TableDefinitionResource getTableDefinition(String tableDefinitionUri) 
      throws ClientWebException;

  /**
   * Assert that a table with the given id and schema exists on the server.
   *
   * @param tableId
   *          the unique identifier of the table
   * @param currentSyncTag
   *          the current SyncTag for the table
   * @param cols
   *          a map from column names to column types, see {@link ColumnType}
   * @return the TableResource for the table (the server may return different
   *         SyncTag values)
   * @throws ClientWebException
   */
  public TableResource createTable(String tableId, String schemaETag, ArrayList<Column> columns)
      throws ClientWebException;

  /**
   * Delete the table with the given id from the server.
   *
   * @param tableId
   *          the unique identifier of the table
   * @throws ClientWebException
   */
  public void deleteTable(String tableId) throws ClientWebException;

  /**
   * Retrieve changes in the server state since the last synchronization.
   *
   * @param tableId
   *          the unique identifier of the table
   * @param schemaETag
   *          tracks the schema instance that this id has
   * @param dataETag
   *          tracks the last dataETag for the last successfully downloaded row
   *          in the table.
   * @return an IncomingModification representing the latest state of the table
   *         on server since the last sync or null if the table does not exist
   *         on the server.
   * @throws ClientWebException
   */
  public IncomingRowModifications getUpdates(String tableId, String schemaETag, String dataETag)
      throws ClientWebException;

  /**
   * Apply updates in a collection up to the server.
   * 
   * @param tableId
   * @param schemaETag
   * @param dataETag
   * @param rowsToInsertOrUpdate
   * @return
   * @throws ClientWebException
   */
  public RowOutcomeList insertOrUpdateRows(String tableId, String schemaETag, String dataETag,
      List<SyncRow> rowsToInsertOrUpdate) throws ClientWebException;

  /**
   * Apply inserts, updates and deletes in a collection up to the server.
   * 
   * @param tableId
   * @param schemaETag
   * @param dataETag
   * @param rowsToInsertOrUpdate
   * @return
   * @throws ClientWebException
   */
  public RowOutcomeList alterRows(String tableId, String schemaETag, String dataETag,
      List<SyncRow> rowsToInsertUpdateOrDelete) throws ClientWebException;

  /**
   * Delete the given row ids from the server.
   *
   * @param tableId
   *          the unique identifier of the table
   * @param schemaETag
   *          tracks the schema instance that this id has
   * @param dataETag
   *          tracks the last dataETag for the last successfully downloaded row
   *          in the table.
   * @param rowToDelete
   *          the row to delete
   * @return a RowModification containing the (rowId, null, table dataETag)
   *         after the modification
   * @throws ClientWebException
   */
  public RowModification deleteRow(String tableId, String schemaETag, String dataETag,
      SyncRow rowToDelete) throws ClientWebException;

  /**
   * Synchronizes the app level files. This includes any files that are not
   * associated with a particular table--i.e. those that are not in the
   * directory appid/tables/. It also excludes those files that are in a set of
   * directories that do not sync--appid/metadata, appid/logging, etc.
   *
   * @param true if local files should be pushed. Otherwise they are only pulled
   *        down.
   * @param SynchronizerStatus
   *          for reporting detailed progress of app-level file sync
   * @return true if successful
   * @throws ClientWebException
   */
  public boolean syncAppLevelFiles(boolean pushLocalFiles, SynchronizerStatus syncStatus)
      throws ClientWebException;

  /**
   * Sync only the files associated with the specified table. This does NOT sync
   * any media files associated with individual rows of the table.
   *
   * @param tableId
   * @param onChange
   *          callback if the assets/csv/tableId.properties.csv file changes
   * @param pushLocal
   *          true if the local files should be pushed
   * @throws ClientWebException
   */
  public void syncTableLevelFiles(String tableId, OnTablePropertiesChanged onChange,
      boolean pushLocal, SynchronizerStatus syncStatus) throws ClientWebException;

  /**
   * Ensure that the file attachments for the indicated row values are pulled
   * down to the local system.
   *
   * @param instanceFileUri
   * @param tableId
   * @param serverRow
   * @param shouldDeleteLocal
   *          - true if all other files in the local instance folder should be
   *          removed.
   * @return true if successful
   * @throws ClientWebException
   */
  public boolean getFileAttachments(String instanceFileUri, String tableId, SyncRow serverRow,
      ArrayList<ColumnDefinition> fileAttachmentColumns, boolean shouldDeleteLocal) throws ClientWebException;

  /**
   * Ensure that the file attachments for the indicated row values exist on the
   * server. File attachments are immutable on the server -- never updated and
   * never destroyed.
   *
   * @param instanceFileUri
   * @param tableId
   * @param localRow
   * @return true if successful
   * @throws ClientWebException
   */
  public boolean putFileAttachments(String instanceFileUri, String tableId, SyncRow localRow,
      ArrayList<ColumnDefinition> fileAttachmentColumns)
      throws ClientWebException;
}
