package org.opendatakit.sync.exceptions;

import java.io.IOException;

public class NoAppNameSpecifiedException extends IOException {

	private static final long serialVersionUID = -5359484141172388331L;

	public NoAppNameSpecifiedException() {
	    super();
	  }

	  /**
	   * @param detailMessage
	   * @param throwable
	   */
	  public NoAppNameSpecifiedException(String detailMessage, Throwable throwable) {
	    super(detailMessage, throwable);
	  }

	  /**
	   * @param detailMessage
	   */
	  public NoAppNameSpecifiedException(String detailMessage) {
	    super(detailMessage);
	  }

	  /**
	   * @param throwable
	   */
	  public NoAppNameSpecifiedException(Throwable throwable) {
	    super(throwable);
	  }

	}