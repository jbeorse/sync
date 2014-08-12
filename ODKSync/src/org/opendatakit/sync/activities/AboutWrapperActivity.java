package org.opendatakit.sync.activities;

import org.opendatakit.sync.R;

import android.app.Activity;
import android.os.Bundle;

public class AboutWrapperActivity extends Activity {
  @Override
  protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    // see if we saved the state
    this.setContentView(R.layout.about_wrapper_activity);
  }

}
