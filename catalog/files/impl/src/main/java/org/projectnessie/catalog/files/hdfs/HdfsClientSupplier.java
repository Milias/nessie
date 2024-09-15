/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.catalog.files.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.projectnessie.catalog.files.config.HdfsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HdfsClientSupplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsClientSupplier.class);

  private final HdfsOptions hdfsOptions;

  public HdfsClientSupplier(HdfsOptions hdfsOptions) {
    this.hdfsOptions = hdfsOptions;
  }

  Configuration buildClientConfiguration() {
    Configuration conf = new Configuration();

    if (hdfsOptions.resourcesConfig().isPresent()) {
      conf.addResource(new Path(hdfsOptions.resourcesConfig().get()));
    }

    return conf;
  }
}
