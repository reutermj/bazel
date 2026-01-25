// Copyright 2025 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.actions;

import java.io.IOException;

/**
 * An interface for reading the contents of artifacts during action template expansion.
 *
 * <p>This is used by {@code template_ctx.read_artifacts()} to read manifest files containing
 * artifact exec paths. The implementation is provided by the Skyframe execution phase when input
 * artifacts have been built.
 */
@FunctionalInterface
public interface ArtifactContentReader {

  /**
   * Reads the contents of the given artifact as a UTF-8 string.
   *
   * @param artifact the artifact to read
   * @return the contents of the artifact as a string
   * @throws IOException if the file cannot be read
   */
  String readContents(Artifact artifact) throws IOException;
}
