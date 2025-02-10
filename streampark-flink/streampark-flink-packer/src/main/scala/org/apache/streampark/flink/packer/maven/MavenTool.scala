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

package org.apache.streampark.flink.packer.maven

import org.apache.streampark.common.conf.{InternalConfigHolder, Workspace}
import org.apache.streampark.common.conf.CommonConfig.{MAVEN_AUTH_PASSWORD, MAVEN_AUTH_USER, MAVEN_REMOTE_URL, MAVEN_SETTINGS_PATH}
import org.apache.streampark.common.util.{Logger, Utils}

import com.google.common.collect.Lists
import org.apache.commons.lang3.StringUtils
import org.apache.maven.plugins.shade.{DefaultShader, ShadeRequest}
import org.apache.maven.plugins.shade.filter.Filter
import org.apache.maven.plugins.shade.resource.{ManifestResourceTransformer, ResourceTransformer, ServicesResourceTransformer}
import org.apache.maven.repository.internal.MavenRepositorySystemUtils
import org.apache.maven.settings.Settings
import org.apache.maven.settings.building.{DefaultSettingsBuilderFactory, DefaultSettingsBuildingRequest}
import org.codehaus.plexus.logging.{Logger => PlexusLog}
import org.codehaus.plexus.logging.console.ConsoleLogger
import org.eclipse.aether.{RepositorySystem, RepositorySystemSession}
import org.eclipse.aether.artifact.DefaultArtifact
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory
import org.eclipse.aether.repository.{LocalRepository, Proxy, RemoteRepository}
import org.eclipse.aether.resolution.{ArtifactDescriptorRequest, ArtifactRequest}
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory
import org.eclipse.aether.spi.connector.transport.TransporterFactory
import org.eclipse.aether.transport.file.FileTransporterFactory
import org.eclipse.aether.transport.http.HttpTransporterFactory
import org.eclipse.aether.util.repository.{AuthenticationBuilder, DefaultMirrorSelector, DefaultProxySelector}

import javax.annotation.{Nonnull, Nullable}

import java.io.File
import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object MavenTool extends Logger {

  private[this] lazy val plexusLog = new ConsoleLogger(PlexusLog.LEVEL_INFO, "streampark-maven")

  /** create maven repository endpoint */
  private[this] lazy val locator = MavenRepositorySystemUtils.newServiceLocator

  /** default maven local repository */
  private[this] lazy val localRepo = new LocalRepository(Workspace.MAVEN_LOCAL_PATH)

  private[this] val excludeArtifact = Set(
    Artifact.of("org.apache.flink:force-shading:*"),
    Artifact.of("org.apache.flink:flink-shaded-force-shading:*"),
    Artifact.of("com.google.code.findbugs:jsr305:*"),
    Artifact.of("org.apache.logging.log4j:*:*")
  )

  private[this] def getRemoteRepos(): List[RemoteRepository] = {
    val builder =
      new RemoteRepository.Builder("central", "default", InternalConfigHolder.get(MAVEN_REMOTE_URL))
    val remoteRepository =
      if (
        InternalConfigHolder.get(MAVEN_AUTH_USER) == null || InternalConfigHolder.get(
          MAVEN_AUTH_PASSWORD) == null
      ) {
        builder.build()
      } else {
        val authentication = new AuthenticationBuilder()
          .addUsername(InternalConfigHolder.get[String](MAVEN_AUTH_USER))
          .addPassword(InternalConfigHolder.get[String](MAVEN_AUTH_PASSWORD))
          .build()
        builder.setAuthentication(authentication).build()
      }
    List(remoteRepository)
  }

  /**
   * Build a fat-jar with custom jar libraries.
   *
   * @param jarLibs
   *   list of jar lib paths for building fat-jar
   * @param outFatJarPath
   *   output paths of fat-jar, like "/streampark/workspace/233/my-fat.jar"
   * @return
   *   File Object of output fat-jar
   */
  @throws[Exception]
  def buildFatJar(
      @Nullable mainClass: String,
      @Nonnull jarLibs: Set[String],
      @Nonnull outFatJarPath: String): File = {
    // check userJarPath
    val uberJar = new File(outFatJarPath)
    require(
      outFatJarPath.endsWith(".jar") && !uberJar.isDirectory,
      s"[StreamPark] streampark-packer: outFatJarPath($outFatJarPath) should be a JAR file.")
    uberJar.delete()
    // resolve all jarLibs
    val jarSet = new util.HashSet[File]
    jarLibs.foreach {
      jar =>
        new File(jar) match {
          case jarFile if jarFile.exists() =>
            if (jarFile.isFile) {
              Try(Utils.checkJarFile(jarFile.toURI.toURL)) match {
                case Success(_) => jarSet.add(jarFile)
                case Failure(e) => logWarn(s"buildFatJar: error, ${e.getMessage}")
              }
            } else {
              jarFile.listFiles.foreach(
                jar => {
                  if (jar.isFile) {
                    Try(Utils.checkJarFile(jar.toURI.toURL)) match {
                      case Success(_) => jarSet.add(jar)
                      case Failure(e) =>
                        logWarn(
                          s"buildFatJar: directory [${jarFile.getAbsolutePath}], error: ${e.getMessage}")
                    }
                  }
                })
            }
          case _ =>
        }
    }

    logInfo(s"start shaded fat-jar: ${jarSet.mkString(",")}")

    // shade jars
    val shadeRequest = {
      val req = new ShadeRequest
      req.setJars(jarSet)
      req.setUberJar(uberJar)

      val transformer = ArrayBuffer[ResourceTransformer]()
      // ref https://ci.apache.org/projects/flink/flink-docs-master/docs/connectors/table/overview/#transform-table-connectorformat-resources
      transformer += new ServicesResourceTransformer()
      if (mainClass != null) {
        val manifest = new ManifestResourceTransformer()
        manifest.setMainClass(mainClass)
        transformer += manifest
      }
      req.setResourceTransformers(transformer.toList)
      // issue: https://github.com/apache/streampark/issues/2350
      req.setFilters(List(new ShadedFilter))
      req.setRelocators(Lists.newArrayList())
      req
    }
    val shader = new DefaultShader()
    shader.enableLogging(plexusLog)
    shader.shade(shadeRequest)
    logInfo(s"finish build fat-jar: ${uberJar.getAbsolutePath}")
    uberJar
  }

  /**
   * Build a fat-jar with custom jar librarties and maven artifacts.
   *
   * @param artifact
   *   maven artifacts and jar libraries for building a fat-jar
   * @param outFatJarPath
   *   output paths of fat-jar, like "/streampark/workspace/233/my-fat.jar"
   */
  @throws[Exception]
  def buildFatJar(
      @Nullable mainClass: String,
      @Nonnull artifact: MavenArtifact,
      @Nonnull outFatJarPath: String): File = {
    val jarLibs = artifact.extJarLibs
    val arts = artifact.mavenArts
    if (jarLibs.isEmpty && arts.isEmpty) {
      throw new Exception(s"[StreamPark] streampark-packer: empty artifacts.")
    }
    val artFilePaths = resolveArtifacts(arts).map(_.getAbsolutePath)
    buildFatJar(mainClass, jarLibs ++ artFilePaths, outFatJarPath)
  }

  def resolveArtifactsAsJava(mavenArtifacts: util.Set[Artifact]): util.Set[File] = resolveArtifacts(
    mavenArtifacts.toSet).asJava

  /**
   * Resolve the collectoin of artifacts, Artifacts will be download to ConfigConst.MAVEN_LOCAL_DIR
   * if necessary. notes: Only compile scope dependencies will be resolved.
   *
   * @param mavenArtifacts
   *   collection of maven artifacts
   * @return
   *   jar File Object of resolved artifacts
   */
  @throws[Exception]
  def resolveArtifacts(mavenArtifacts: Set[Artifact]): Set[File] = {
    if (mavenArtifacts == null) {
      return Set.empty[File]
    }

    val (repoSystem, session) = getRepositoryEndpoint()

    val exclusions = mavenArtifacts
      .flatMap(_.extensions.map(_.split(":")))
      .map(a => Artifact(a.head, a.last, null)) ++ excludeArtifact

    val remoteRepos = getRemoteRepos()

    val exclusionAll = exclusions.exists(e => e.groupId == "*" && e.artifactId == "*")

    val artifacts = mavenArtifacts.map(
      e => new DefaultArtifact(e.groupId, e.artifactId, e.classifier, "jar", e.version))

    // read relevant artifact descriptor info and excluding items if necessary.
    val dependencies =
      if (exclusionAll) Set.empty[DefaultArtifact]
      else {
        artifacts
          .map(artifact => new ArtifactDescriptorRequest(artifact, remoteRepos, null))
          .map(descReq => repoSystem.readArtifactDescriptor(session, descReq))
          .flatMap(_.getDependencies)
          .filter(_.getScope == "compile")
          .filter(dep => !exclusions.exists(_.filter(dep.getArtifact)))
          .map(_.getArtifact)
      }

    val mergedArtifacts = artifacts ++ dependencies

    logInfo(
      s"""
         |start resolving dependencies...
         |--------------------------------------------------------------------------------
         ||User-declared dependencies list：
         |${artifacts.mkString(",\n")}
         |
         ||Indirect dependencies list:
         |${dependencies.mkString(",\n")}
         |
         ||Exclusion indirect dependencies list:
         |${exclusions.map(x => s"${x.groupId}:${x.artifactId}").mkString(",\n")}
         |
         ||Final dependencies list:
         |${mergedArtifacts.mkString(",\n")}
         |--------------------------------------------------------------------------------
         |""".stripMargin
    )

    // download artifacts
    val artReq = mergedArtifacts.map(a => new ArtifactRequest(a, remoteRepos, null))

    repoSystem
      .resolveArtifacts(session, artReq)
      .map(_.getArtifact.getFile)
      .toSet
  }

  /** create composite maven endpoint */
  private[this] def getRepositoryEndpoint(): (RepositorySystem, RepositorySystemSession) = {

    def newRepoSystem(): RepositorySystem = {
      locator.addService(
        classOf[RepositoryConnectorFactory],
        classOf[BasicRepositoryConnectorFactory])
      locator.addService(classOf[TransporterFactory], classOf[FileTransporterFactory])
      locator.addService(classOf[TransporterFactory], classOf[HttpTransporterFactory])
      locator.getService(classOf[RepositorySystem])
    }

    /** create maven repository session endpoint */
    def newSession(system: RepositorySystem): RepositorySystemSession = {
      val session = MavenRepositorySystemUtils.newSession
      session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo))

      val setting = getSettings()
      if (setting != null) {
        // 1) mirror
        val mirrors = setting.getMirrors
        if (mirrors.nonEmpty) {
          val defaultMirror = new DefaultMirrorSelector()
          setting.getMirrors.foreach {
            mirror =>
              defaultMirror.add(
                mirror.getId,
                mirror.getUrl,
                "default",
                false,
                mirror.getMirrorOf,
                null)
          }
          session.setMirrorSelector(defaultMirror)
        }

        // 2) proxy
        val proxies = setting.getProxies
        if (proxies.nonEmpty && proxies.exists(_.isActive)) {
          val proxySelector = new DefaultProxySelector()
          proxies.foreach(
            p => {
              if (p.isActive) {
                val proxy = new Proxy(p.getProtocol, p.getHost, p.getPort)
                proxySelector.add(proxy, p.getNonProxyHosts)
              }
            })
          session.setProxySelector(proxySelector)
        }
      }
      session
    }

    val repoSystem = newRepoSystem()
    val session = newSession(repoSystem)
    (repoSystem, session)
  }

  private def getSettings(): Settings = {
    val settingPath = InternalConfigHolder.get[String](MAVEN_SETTINGS_PATH)
    if (StringUtils.isNotBlank(settingPath)) {
      val settingFile = new File(settingPath)
      if (settingFile.exists() && settingFile.isFile) {
        val settingsBuilderFactory = new DefaultSettingsBuilderFactory()
        val settingsBuilder = settingsBuilderFactory.newInstance
        val settingRequest = new DefaultSettingsBuildingRequest()
        settingRequest.setGlobalSettingsFile(settingFile)
        settingRequest.setUserSettingsFile(settingFile)
        return settingsBuilder.build(settingRequest).getEffectiveSettings
      }
    }
    null
  }

  private class ShadedFilter extends Filter {
    override def canFilter(jar: File): Boolean = true

    override def isFiltered(name: String): Boolean = {
      if (name.startsWith("META-INF/")) {
        if (name.endsWith(".SF") || name.endsWith(".DSA") || name.endsWith(".RSA")) {
          logInfo(s"shaded ignore file: $name")
          return true
        }
      }
      false
    }

    override def finished(): Unit = {}
  }
}
