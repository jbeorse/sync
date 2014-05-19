package org.opendatakit.sync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An object for measuring the results of a synchronization call. This is
 * especially intended to see how to display the results to the user. For
 * example, imagine you wanted to synchronize three tables. The object should
 * contain three TableResult objects, mapping the dbTableName to the
 * status corresponding to outcome.
 * @author sudar.sam@gmail.com
 *
 */
public class SynchronizationResult {

  /**
   * Status code used at app and table levels
   *
   * WORKING should never be returned (the initial value)
   */
  public enum Status {
    WORKING,
    SUCCESS,
    FAILURE,
    EXCEPTION,
    TABLE_DOES_NOT_EXIST_ON_SERVER,
    TABLE_CONTAINS_CHECKPOINTS,
    TABLE_CONTAINS_CONFLICTS,
    TABLE_REQUIRES_APP_LEVEL_SYNC;
  }

  private Status appLevelStatus = Status.WORKING;

  private final Map<String, TableResult> mResults = new HashMap<String, TableResult>();

  public SynchronizationResult() {
  }

  public Status getAppLevelStatus() {
    return appLevelStatus;
  }

  public void setAppLevelStatus(Status status) {
    this.appLevelStatus = status;
  }

  /**
   * Get all the {@link TableResult} objects in this result.
   * @return
   */
  public List<TableResult> getTableResults() {
    List<TableResult> r = new ArrayList<TableResult>();
    r.addAll(this.mResults.values());
    Collections.sort(r, new Comparator<TableResult>(){
      @Override
      public int compare(TableResult lhs, TableResult rhs) {
        return lhs.getTableId().compareTo(rhs.getTableId());
      }});

    return r;
  }

  public TableResult getTableResult(String tableId) {
    TableResult r = mResults.get(tableId);
    if ( r == null ) {
      r = new TableResult(tableId);
      mResults.put(tableId, r);
    }
    return r;
  }

}
