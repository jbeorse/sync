/*
 * Copyright (C) 2014 University of Washington
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
package org.opendatakit.sync.springframework;

import java.util.ArrayList;
import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;

public class TextPlainHttpMessageConverter extends StringHttpMessageConverter {

  @Override
  public List<MediaType> getSupportedMediaTypes() {
    // The default implementation of this includes */* according to the doc at:
    // http://static.springsource.org/spring/docs/3.0.x/javadoc-api/org/springframework/http/converter/StringHttpMessageConverter.html
    // This is bad, as we are using xml message converters as well. So instead
    // we're going to ONLY support Text/Plain.
    List<MediaType> onlyType = new ArrayList<MediaType>();
    onlyType.add(MediaType.TEXT_PLAIN);
    return onlyType;
  }

}
