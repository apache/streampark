/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.core.util;

import org.apache.streampark.common.enums.ApplicationType;
import org.apache.streampark.common.enums.FlinkDeployMode;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.entity.FlinkApplication;
import org.apache.streampark.console.core.entity.Resource;
import org.apache.streampark.console.core.service.ResourceService;
import org.apache.streampark.flink.packer.pipeline.BuildResult;
import org.apache.streampark.flink.packer.pipeline.DockerImageBuildResponse;
import org.apache.streampark.flink.packer.pipeline.ShadedBuildResponse;
import org.apache.streampark.flink.packer.pipeline.SimpleBuildResponse;

import org.springframework.util.StreamUtils;

import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Creates a bounded ZIP view of a successful Flink application build. */
public final class FlinkApplicationArtifactExporter {

    private static final String ZIP_SUFFIX = "-dependencies.zip";

    private FlinkApplicationArtifactExporter() {
    }

    /**
     * Writes the complete archive only after every source file has been read successfully.
     *
     * @param application application whose artifact is requested
     * @param result successful build result persisted for the application
     * @param resourceService resource lookup used by Apache Flink upload fallback
     * @param response servlet response
     * @param validateBuild checks that the build is unchanged before committing the response
     * @throws IOException when the source or response cannot be read or written
     */
    public static void write(
                             FlinkApplication application,
                             BuildResult result,
                             ResourceService resourceService,
                             HttpServletResponse response,
                             Runnable validateBuild) throws IOException {
        write(application, result, resourceService, response, validateBuild, FsOperator.lfs(), FsOperator.hdfs());
    }

    static void write(
                      FlinkApplication application,
                      BuildResult result,
                      ResourceService resourceService,
                      HttpServletResponse response,
                      Runnable validateBuild,
                      FsOperator localFs,
                      FsOperator remoteFs) throws IOException {
        String archiveName = "app-" + application.getId() + ZIP_SUFFIX;
        Path archive = Files.createTempFile("streampark-artifact-", ".zip");
        Throwable failure = null;
        try {
            try {
                createArchive(
                    application,
                    result,
                    resourceService,
                    archive,
                    archiveName.substring(0, archiveName.length() - 4),
                    localFs,
                    remoteFs);
            } catch (ApiAlertException e) {
                throw e;
            } catch (IOException e) {
                throw new ApiAlertException("The job artifact could not be read.", e);
            }
            validateBuild.run();
            response.setContentType("application/zip");
            response.setHeader(
                "Content-Disposition", "attachment; filename=\"" + archiveName + "\"");
            response.setHeader("Cache-Control", "no-store");
            response.setContentLengthLong(Files.size(archive));
            try (
                InputStream input = Files.newInputStream(archive);
                OutputStream output = response.getOutputStream()) {
                StreamUtils.copy(input, output);
            }
        } catch (IOException | RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            try {
                Files.deleteIfExists(archive);
            } catch (IOException cleanupFailure) {
                if (failure == null) {
                    throw cleanupFailure;
                }
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    static void createArchive(
                              FlinkApplication application,
                              BuildResult result,
                              ResourceService resourceService,
                              Path archive,
                              String archiveRoot) throws IOException {
        createArchive(
            application,
            result,
            resourceService,
            archive,
            archiveRoot,
            FsOperator.lfs(),
            FsOperator.hdfs());
    }

    static void createArchive(
                              FlinkApplication application,
                              BuildResult result,
                              ResourceService resourceService,
                              Path archive,
                              String archiveRoot,
                              FsOperator localFs,
                              FsOperator remoteFs) throws IOException {
        if (application.getId() == null || result == null || !result.pass()) {
            throw unavailable();
        }
        List<Source> sources = resolveSources(application, result, resourceService, localFs, remoteFs);
        if (sources.isEmpty()) {
            throw unavailable();
        }
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive))) {
            zip.setLevel(Deflater.NO_COMPRESSION);
            zip.putNextEntry(new ZipEntry(archiveRoot + "/"));
            zip.closeEntry();
            for (Source source : sources) {
                writeSource(zip, archiveRoot, source);
            }
        }
    }

    private static List<Source> resolveSources(
                                               FlinkApplication application,
                                               BuildResult result,
                                               ResourceService resourceService,
                                               FsOperator localFs,
                                               FsOperator remoteFs) throws IOException {
        FlinkDeployMode deployMode = application.getDeployModeEnum();
        if (deployMode == FlinkDeployMode.YARN_APPLICATION && result instanceof SimpleBuildResponse) {
            return resolveYarnSources(application, resourceService, remoteFs);
        }
        if (!(result instanceof ShadedBuildResponse || result instanceof DockerImageBuildResponse)) {
            throw unavailable();
        }
        String workspace = workspacePath(result);
        String canonicalWorkspace = requireLocalWorkspace(localFs, application, workspace);
        List<Source> sources = new ArrayList<>();
        String mainPath;
        if (result instanceof ShadedBuildResponse) {
            ShadedBuildResponse shaded = (ShadedBuildResponse) result;
            mainPath = addLocalMain(
                localFs,
                canonicalWorkspace,
                shaded.shadedJarPath(),
                application,
                resourceService,
                sources);
        } else if (result instanceof DockerImageBuildResponse) {
            DockerImageBuildResponse docker = (DockerImageBuildResponse) result;
            mainPath = addLocalMain(
                localFs,
                canonicalWorkspace,
                fileName(docker.dockerInnerMainJarPath()),
                application,
                resourceService,
                sources);
        } else {
            throw unavailable();
        }
        // Native Session only produces the shaded JAR.
        if (deployMode == FlinkDeployMode.KUBERNETES_NATIVE_SESSION) {
            return sources;
        }
        // Docker builds stage lib/ even when empty. Standalone SQL builds stage runtime JARs.
        boolean requireLibDirectory = result instanceof DockerImageBuildResponse || application.isFlinkSqlJob();
        addDependencies(localFs, join(canonicalWorkspace, "lib"), sources, mainPath, requireLibDirectory);
        return sources;
    }

    private static List<Source> resolveYarnSources(
                                                   FlinkApplication application,
                                                   ResourceService resourceService,
                                                   FsOperator fs) throws IOException {
        List<Source> sources = new ArrayList<>();
        if (!application.isFlinkSqlJob() && !application.isFlinkJar()) {
            throw unavailable();
        }
        if (application.isFlinkJar()) {
            String mainName = resolveYarnMainName(application, resourceService, fs);
            if (!safeJarName(mainName)) {
                throw unavailable();
            }
            String mainDirectory = application.isStreamParkType()
                ? application.getAppLib()
                : application.getAppHome();
            String main = join(mainDirectory, mainName);
            if (!fs.isFile(main)) {
                throw unavailable();
            }
            sources.add(new Source(fs, main, mainName));
            addDependencies(fs, application.getAppLib(), sources, main, false);
            return sources;
        }
        addDependencies(fs, application.getAppLib(), sources, null, true);
        return sources;
    }

    private static String resolveYarnMainName(
                                              FlinkApplication application,
                                              ResourceService resourceService,
                                              FsOperator fs) throws IOException {
        if (application.isStreamParkType()) {
            if (application.getModule() == null) {
                throw unavailable();
            }
            String mainName = application.getModule() + ".jar";
            if (!safeJarName(mainName)) {
                throw unavailable();
            }
            return mainName;
        }
        ApiAlertException.throwIfFalse(
            application.getApplicationType() == ApplicationType.APACHE_FLINK,
            "No downloadable job artifact is available.");
        if (!safeJarName(application.getJar())) {
            throw unavailable();
        }
        String configuredName = application.getJar();
        if (fs.isFile(join(application.getAppHome(), configuredName))) {
            return configuredName;
        }
        if (resourceService == null) {
            throw unavailable();
        }
        Resource resource = resourceService.findByResourceName(application.getTeamId(), configuredName);
        if (resource == null || resource.getFilePath() == null) {
            throw unavailable();
        }
        String resourceName = fileName(resource.getFilePath());
        if (!safeJarName(resourceName)
            || !fs.isFile(join(application.getAppHome(), resourceName))) {
            throw unavailable();
        }
        return resourceName;
    }

    private static String workspacePath(BuildResult result) {
        String workspace;
        if (result instanceof ShadedBuildResponse) {
            workspace = ((ShadedBuildResponse) result).workspacePath();
        } else if (result instanceof DockerImageBuildResponse) {
            workspace = ((DockerImageBuildResponse) result).workspacePath();
        } else {
            throw unavailable();
        }
        if (workspace == null || workspace.trim().isEmpty()) {
            throw unavailable();
        }
        return workspace;
    }

    private static String requireLocalWorkspace(
                                                FsOperator fs,
                                                FlinkApplication application,
                                                String workspace) throws IOException {
        Path root = localPath(fs.canonicalPath(application.getLocalAppHome()));
        Path path = localPath(fs.canonicalPath(workspace));
        if (!path.startsWith(root)) {
            throw unavailable();
        }
        return path.toString();
    }

    private static String addLocalMain(
                                       FsOperator fs,
                                       String workspace,
                                       String path,
                                       FlinkApplication application,
                                       ResourceService resourceService,
                                       List<Source> sources) throws IOException {
        if (!safeJarName(fileName(path))) {
            throw unavailable();
        }
        Path root = localPath(workspace);
        Path candidate;
        try {
            candidate = Paths.get(path);
        } catch (RuntimeException e) {
            throw unavailable();
        }
        candidate = candidate.isAbsolute()
            ? candidate.toAbsolutePath().normalize()
            : root.resolve(candidate).normalize();
        if (!safeJarName(candidate.getFileName().toString())) {
            throw unavailable();
        }
        if (!candidate.startsWith(root)) {
            if (!root.equals(localPath(fs.canonicalPath(application.getLocalAppHome())))
                || !isUploadedResourceMain(application, resourceService, candidate)) {
                throw unavailable();
            }
            // Build preparation copies the resolved upload into the application's workspace.
            candidate = root.resolve(candidate.getFileName());
        }
        Path canonicalCandidate = localPath(fs.canonicalPath(candidate.toString()));
        if (!canonicalCandidate.startsWith(root) || !fs.isFile(canonicalCandidate.toString())) {
            throw unavailable();
        }
        sources.add(new Source(fs, canonicalCandidate.toString(), canonicalCandidate.getFileName().toString()));
        return canonicalCandidate.toString();
    }

    private static boolean isUploadedResourceMain(
                                                  FlinkApplication application,
                                                  ResourceService resourceService,
                                                  Path source) {
        Long teamId = application.getTeamId();
        if (teamId == null
            || resourceService == null
            || !application.isFlinkJar()
            || !application.isUploadResource()
            || application.getApplicationType() != ApplicationType.APACHE_FLINK
            || !safeJarName(application.getJar())) {
            return false;
        }
        Resource resource = resourceService.findByResourceName(teamId, application.getJar());
        return resource != null
            && teamId.equals(resource.getTeamId())
            && application.getJar().equals(resource.getResourceName())
            && resource.getFilePath() != null
            && source.equals(localPath(resource.getFilePath()));
    }

    private static void addDependencies(
                                        FsOperator fs,
                                        String directory,
                                        List<Source> sources,
                                        String excludedPath,
                                        boolean requireDirectory) throws IOException {
        String canonicalDirectory;
        try {
            canonicalDirectory = fs.canonicalPath(directory);
        } catch (FileNotFoundException | NoSuchFileException e) {
            if (requireDirectory) {
                throw unavailable();
            }
            return;
        }
        if (!fs.isDirectory(canonicalDirectory)) {
            throw unavailable();
        }
        List<String> entries = fs.list(canonicalDirectory);
        entries.sort(Comparator.comparing(FlinkApplicationArtifactExporter::fileName));
        String canonicalExcluded = excludedPath == null ? null : fs.canonicalPath(excludedPath);
        for (String entry : entries) {
            String name = fileName(entry);
            if (!hasJarSuffix(name)) {
                continue;
            }
            if (!safeJarName(name) || !fs.isFile(entry)) {
                throw unavailable();
            }
            if (canonicalExcluded == null
                || !canonicalExcluded.equals(fs.canonicalPath(entry))) {
                sources.add(new Source(fs, entry, "lib/" + name));
            }
        }
    }

    private static void writeSource(ZipOutputStream zip, String root, Source source) throws IOException {
        try (InputStream input = source.fs.open(source.path)) {
            zip.putNextEntry(new ZipEntry(root + "/" + source.entryName));
            StreamUtils.copy(input, zip);
            zip.closeEntry();
        } catch (IOException e) {
            throw new IOException("Failed to read job artifact", e);
        }
    }

    private static Path localPath(String path) {
        try {
            return Paths.get(path).toAbsolutePath().normalize();
        } catch (RuntimeException e) {
            throw unavailable();
        }
    }

    private static String join(String parent, String child) {
        if (parent == null || child == null || child.contains("/") || child.contains("\\")) {
            throw unavailable();
        }
        return parent.endsWith("/") ? parent + child : parent + "/" + child;
    }

    private static String fileName(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        int slash = path.lastIndexOf('/');
        return slash >= 0 ? path.substring(slash + 1) : new File(path).getName();
    }

    private static boolean safeJarName(String name) {
        return hasJarSuffix(name) && name.matches("[^/\\\\:\\p{Cntrl}]+");
    }

    private static boolean hasJarSuffix(String name) {
        return name != null && name.toLowerCase(Locale.ROOT).endsWith(".jar");
    }

    private static ApiAlertException unavailable() {
        return new ApiAlertException("No downloadable job artifact is available.");
    }

    private static final class Source {

        private final FsOperator fs;
        private final String path;
        private final String entryName;

        private Source(FsOperator fs, String path, String entryName) {
            this.fs = fs;
            this.path = path;
            this.entryName = entryName;
        }
    }
}
