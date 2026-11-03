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

package org.apache.streampark.flink.packer.maven;

import org.apache.streampark.common.configuration.Configuration;
import org.apache.streampark.common.configuration.Constants;
import org.apache.streampark.common.configuration.GlobalConfiguration;
import org.apache.streampark.common.configuration.Workspace;
import org.apache.streampark.common.configuration.option.MavenOptions;
import org.apache.streampark.common.util.AssertUtils;
import org.apache.streampark.common.util.LoggerSupport;
import org.apache.streampark.common.util.Utils;

import org.apache.maven.plugins.shade.DefaultShader;
import org.apache.maven.plugins.shade.ShadeRequest;
import org.apache.maven.plugins.shade.filter.Filter;
import org.apache.maven.plugins.shade.resource.ManifestResourceTransformer;
import org.apache.maven.plugins.shade.resource.ResourceTransformer;
import org.apache.maven.plugins.shade.resource.ServicesResourceTransformer;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;

import com.google.common.collect.Lists;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactDescriptorRequest;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.repository.AuthenticationBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Maven build utilities. */
public final class MavenTool extends LoggerSupport {

    private static final MavenTool INSTANCE = new MavenTool();

    private static final org.codehaus.plexus.logging.Logger PLEXUS_LOG =
        new ConsoleLogger(org.codehaus.plexus.logging.Logger.LEVEL_INFO, "streampark-maven");

    private static final List<Artifact> EXCLUDE_ARTIFACT =
        Arrays.asList(
            Artifact.of("org.apache.flink:force-shading:*"),
            Artifact.of("com.google.code.findbugs:jsr305:*"),
            Artifact.of("org.apache.logging.log4j:*:*"));

    private MavenTool() {
    }

    private static List<RemoteRepository> getRemoteRepos() {
        Configuration configuration = GlobalConfiguration.current();
        RemoteRepository.Builder builder =
            new RemoteRepository.Builder(
                "central",
                Constants.DEFAULT,
                configuration.get(MavenOptions.REPOSITORY_URL));
        RemoteRepository remoteRepository;
        String username = configuration.getOptional(MavenOptions.USER_NAME).orElse(null);
        String password = configuration.getOptional(MavenOptions.PASSWORD).orElse(null);
        if (username == null || password == null) {
            remoteRepository = builder.build();
        } else {
            org.eclipse.aether.repository.Authentication authentication =
                new AuthenticationBuilder()
                    .addUsername(username)
                    .addPassword(password)
                    .build();
            remoteRepository = builder.setAuthentication(authentication).build();
        }
        return Collections.singletonList(remoteRepository);
    }

    private static boolean isJarFile(File file) {
        if (!file.isFile()) {
            return false;
        }
        try {
            Utils.requireCheckJarFile(file.toURI().toURL());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Build a fat-jar with custom jar libraries.
     *
     * @param mainClass main class name
     * @param jarLibs list of jar lib paths for building fat-jar
     * @param outFatJarPath output paths of fat-jar
     * @return File Object of output fat-jar
     */
    @Nonnull
    public static File buildFatJar(
                                   @Nullable String mainClass,
                                   @Nonnull Set<String> jarLibs,
                                   @Nonnull String outFatJarPath) throws Exception {
        return INSTANCE.buildFatJarInternal(mainClass, jarLibs, outFatJarPath);
    }

    private File buildFatJarInternal(
                                     @Nullable String mainClass,
                                     @Nonnull Set<String> jarLibs,
                                     @Nonnull String outFatJarPath) throws Exception {
        File uberJar = new File(outFatJarPath);
        if (!outFatJarPath.endsWith(Constants.JAR_SUFFIX) || uberJar.isDirectory()) {
            throw new IllegalArgumentException(
                "[StreamPark] streampark-packer: outFatJarPath("
                    + outFatJarPath
                    + ") should be a JAR file.");
        }
        if (uberJar.exists() && !uberJar.delete()) {
            throw new java.io.IOException(
                "[StreamPark] streampark-packer: failed to delete existing uber jar: "
                    + outFatJarPath);
        }
        Set<File> jarSet = new HashSet<>();
        for (String lib : jarLibs) {
            File libFile = new File(lib);
            if (!libFile.exists()) {
                continue;
            }
            if (isJarFile(libFile)) {
                jarSet.add(libFile);
            } else if (libFile.isDirectory()) {
                File[] files = libFile.listFiles();
                if (files != null) {
                    for (File f : files) {
                        if (isJarFile(f)) {
                            jarSet.add(f);
                        }
                    }
                }
            }
        }
        logInfo("start shaded fat-jar: " + String.join(",", jarLibs));
        ShadeRequest shadeRequest = new ShadeRequest();
        // The shader keeps the first duplicate class. Target shims must override the
        // baseline classes embedded in the SQL client and the shared shim artifact.
        shadeRequest.setJars(jarSet.stream()
            .sorted(Comparator.comparingInt(MavenTool::shimPriority)
                .thenComparing(File::getAbsolutePath))
            .collect(Collectors.toCollection(LinkedHashSet::new)));
        shadeRequest.setUberJar(uberJar);
        List<ResourceTransformer> transformer = new ArrayList<>();
        transformer.add(new ServicesResourceTransformer());
        if (mainClass != null) {
            ManifestResourceTransformer manifest = new ManifestResourceTransformer();
            manifest.setMainClass(mainClass);
            transformer.add(manifest);
        }
        shadeRequest.setResourceTransformers(transformer);
        shadeRequest.setFilters(Collections.singletonList(new ShadeFilter()));
        shadeRequest.setRelocators(Lists.newArrayList());
        DefaultShader shader = new DefaultShader();
        shader.enableLogging(PLEXUS_LOG);
        shader.shade(shadeRequest);
        logInfo("finish build fat-jar: " + uberJar.getAbsolutePath());
        return uberJar;
    }

    private static int shimPriority(File jar) {
        if (jar.getName().startsWith("streampark-flink-shims_flink-")) {
            return 0;
        }
        if (jar.getName().startsWith("streampark-flink-shims-base-")) {
            return 1;
        }
        return 2;
    }

    /**
     * Build a fat-jar with custom jar libraries and maven artifacts.
     *
     * @param mainClass main class name
     * @param dependencyInfo maven artifacts and jar libraries for building a fat-jar
     * @param outFatJarPath output paths of fat-jar
     */
    @Nonnull
    public static File buildFatJar(
                                   @Nullable String mainClass,
                                   @Nonnull DependencyInfo dependencyInfo,
                                   @Nonnull String outFatJarPath) throws Exception {
        Set<String> jarLibs = dependencyInfo.extJarLibs();
        Set<Artifact> arts = dependencyInfo.mavenArts();
        AssertUtils.required(
            !(jarLibs.isEmpty() && arts.isEmpty()),
            "[StreamPark] streampark-packer: empty artifacts.");
        List<String> artFilePaths =
            resolveArtifacts(arts).stream().map(File::getAbsolutePath).collect(Collectors.toList());
        Set<String> allJars = new HashSet<>(jarLibs);
        allJars.addAll(artFilePaths);
        return INSTANCE.buildFatJarInternal(mainClass, allJars, outFatJarPath);
    }

    @Nonnull
    public static List<File> resolveArtifacts(Artifact mavenArtifact) throws Exception {
        return resolveArtifacts(Collections.singleton(mavenArtifact));
    }

    /**
     * Resolve the collection of artifacts. Artifacts will be download to local maven repo if
     * necessary. Only compile scope dependencies will be resolved.
     */
    @Nonnull
    public static List<File> resolveArtifacts(@Nullable Set<Artifact> mavenArtifacts) throws Exception {
        if (mavenArtifacts == null) {
            return Collections.emptyList();
        }
        RepositorySystem repoSystem;
        RepositorySystemSession session;
        MavenEndpoint endpoint = getMavenEndpoint();
        repoSystem = endpoint.repoSystem;
        session = endpoint.session;

        List<org.eclipse.aether.artifact.Artifact> artifacts = new ArrayList<>();
        for (Artifact e : mavenArtifacts) {
            org.eclipse.aether.artifact.Artifact artifact =
                new DefaultArtifact(e.groupId(), e.artifactId(), e.classifier(), "jar", e.version());
            artifacts.add(artifact);
        }
        INSTANCE.logInfo("start resolving dependencies: " + artifacts);

        List<RemoteRepository> remoteRepos = getRemoteRepos();
        List<org.eclipse.aether.artifact.Artifact> resolvedArtifacts = new ArrayList<>();
        for (org.eclipse.aether.artifact.Artifact artifact : artifacts) {
            ArtifactDescriptorRequest artDescReq =
                new ArtifactDescriptorRequest(artifact, remoteRepos, null);
            resolvedArtifacts.addAll(
                repoSystem.readArtifactDescriptor(session, artDescReq).getDependencies().stream()
                    .filter(dep -> "compile".equals(dep.getScope()))
                    .filter(
                        dep -> EXCLUDE_ARTIFACT.stream()
                            .noneMatch(ex -> ex.eq(dep.getArtifact())))
                    .map(dep -> dep.getArtifact())
                    .collect(Collectors.toList()));
        }

        List<org.eclipse.aether.artifact.Artifact> mergedArtifacts = new ArrayList<>(artifacts);
        mergedArtifacts.addAll(resolvedArtifacts);
        INSTANCE.logInfo("resolved dependencies: " + mergedArtifacts);

        List<ArtifactRequest> artReqs =
            mergedArtifacts.stream()
                .map(artifact -> new ArtifactRequest(artifact, remoteRepos, null))
                .collect(Collectors.toList());
        return repoSystem.resolveArtifacts(session, artReqs).stream()
            .map(result -> result.getArtifact().getFile())
            .collect(Collectors.toList());
    }

    private static MavenEndpoint getMavenEndpoint() {
        org.eclipse.aether.impl.DefaultServiceLocator locator =
            MavenRepositorySystemUtils.newServiceLocator();
        LocalRepository localRepo = new LocalRepository(Workspace.MAVEN_LOCAL_PATH);

        locator.addService(
            RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        RepositorySystem repoSystem = locator.getService(RepositorySystem.class);

        org.eclipse.aether.DefaultRepositorySystemSession session =
            MavenRepositorySystemUtils.newSession();
        session.setLocalRepositoryManager(
            repoSystem.newLocalRepositoryManager(session, localRepo));
        return new MavenEndpoint(repoSystem, session);
    }

    private static final class MavenEndpoint {

        private final RepositorySystem repoSystem;
        private final RepositorySystemSession session;

        private MavenEndpoint(RepositorySystem repoSystem, RepositorySystemSession session) {
            this.repoSystem = repoSystem;
            this.session = session;
        }
    }

    private static final class ShadeFilter implements Filter {

        @Override
        public boolean canFilter(File jar) {
            return true;
        }

        @Override
        public boolean isFiltered(String name) {
            boolean isFilteredState =
                name.startsWith("META-INF/") && name.endsWith(".SF")
                    || name.endsWith(".DSA")
                    || name.endsWith(".RSA");
            if (isFilteredState) {
                INSTANCE.logInfo("shade ignore file: " + name);
                return true;
            }
            return false;
        }

        @Override
        public void finished() {
        }
    }
}
