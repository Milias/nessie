/*
 * Copyright (C) 2023 Dremio
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.immutables.value.Value;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.storage.uri.StorageUri;

public class HdfsObjectIO implements ObjectIO {
  @Value.Default
  Configuration hadoopConfiguration() {
    return new Configuration();
  }

  private Path filePath(StorageUri uri) {
    return new Path(uri.location());
  }

  private FileSystem fileSystem(StorageUri uri) throws IOException {
    Path path = filePath(uri);
    return path.getFileSystem(hadoopConfiguration());
  }

  @Override
  public void ping(StorageUri uri) throws IOException {
    Path path = filePath(uri);

    try (FileSystem fs = fileSystem(uri)) {
      if (!fs.exists(path)) {
        throw new FileNotFoundException(path.toString());
      }
    }
  }

  @Override
  public InputStream readObject(StorageUri uri) throws IOException {
    Path path = filePath(uri);

    try (FileSystem fs = fileSystem(uri)) {
      return new BufferedInputStream(fs.open(path));
    }
  }

  @Override
  public OutputStream writeObject(StorageUri uri) throws IOException {
    Path path = filePath(uri);

    try (FileSystem fs = fileSystem(uri)) {
      fs.mkdirs(path.getParent());
      return new BufferedOutputStream(fs.create(path));
    }
  }

  @Override
  public void deleteObjects(List<StorageUri> uris) throws IOException {
    IOException ex = null;
    for (StorageUri uri : uris) {
      Path path = filePath(uri);
      try (FileSystem fs = fileSystem(uri)) {
        fs.delete(path, true);
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public void configureIcebergWarehouse(
      StorageUri warehouse,
      BiConsumer<String, String> defaultConfig,
      BiConsumer<String, String> configOverride) {}

  @Override
  public void configureIcebergTable(
      StorageLocations storageLocations,
      BiConsumer<String, String> config,
      BooleanSupplier enableRequestSigning,
      boolean canDoCredentialsVending) {}

  @Override
  public void trinoSampleConfig(
      StorageUri warehouse,
      Map<String, String> icebergConfig,
      BiConsumer<String, String> properties) {}
}
