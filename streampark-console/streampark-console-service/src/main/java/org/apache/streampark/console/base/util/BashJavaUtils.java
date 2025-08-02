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

package org.apache.streampark.console.base.util;

import org.apache.streampark.common.conf.FlinkVersion;
import org.apache.streampark.common.util.FileUtils;
import org.apache.streampark.common.util.PropertiesUtils;

import org.apache.commons.io.output.NullOutputStream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Map;

public class BashJavaUtils {

    private static final String localhost = "localhost";

    public static void main(String[] args) throws IOException {
        String action = args[0].toLowerCase();
        String[] actionArgs = Arrays.copyOfRange(args, 1, args.length);

        switch (action) {
            case "--get_yaml":
                String key = actionArgs[0];
                String conf = actionArgs[1];
                Map<String, String> confMap = PropertiesUtils.fromYamlFileAsJava(conf);
                String value = confMap.get(key);
                System.out.println(value);
                break;
            case "--check_port":
                int port = Integer.parseInt(actionArgs[0]);
                try (Socket ignored = new Socket(localhost, port)) {
                    System.out.println("used");
                } catch (Exception e) {
                    System.out.println("free");
                }
                break;
            case "--free_port":
                int start = Integer.parseInt(actionArgs[0]);
                for (port = start; port < 65535; port++) {
                    try (Socket ignored = new Socket(localhost, port)) {
                    } catch (Exception e) {
                        System.out.println(port);
                        break;
                    }
                }
                break;
            case "--read_flink":
                String input = actionArgs[0];
                String[] inputs = input.split(":");
                String flinkDist = Arrays.stream(inputs).filter(c -> c.contains("flink-dist-")).findFirst().get();
                File flinkHome = new File(flinkDist.replaceAll("/lib/.*", ""));
                FlinkVersion flinkVersion = new FlinkVersion(flinkHome.getAbsolutePath());

                PrintStream originalOut = System.out;
                System.setOut(new PrintStream(new NullOutputStream()));

                String version = flinkVersion.majorVersion();
                float ver = Float.parseFloat(version);
                File yaml = new File(flinkHome, ver < 1.19f ? "/conf/flink-conf.yaml" : "/conf/config.yaml");

                Map<String, String> config = PropertiesUtils.fromYamlFileAsJava(yaml.getAbsolutePath());
                String flinkPort = config.getOrDefault("rest.port", "8081");
                System.setOut(originalOut);
                System.out.println(
                    flinkHome
                        .getAbsolutePath()
                        .concat(",")
                        .concat(flinkHome.getName())
                        .concat(",")
                        .concat(flinkPort));
                break;
            case "--replace":
                String filePath = actionArgs[0];
                String[] text = actionArgs[1].split("\\|\\|");
                String searchText = text[0];
                String replaceText = text[1];
                try {
                    File file = new File(filePath);
                    String content = FileUtils.readString(file);
                    content = content.replace(searchText, replaceText);
                    FileWriter writer = new FileWriter(filePath);
                    writer.write(content);
                    writer.flush();
                    writer.close();
                    System.exit(0);
                } catch (IOException e) {
                    System.exit(1);
                }
                break;
            case "--download":
                try {
                    URL url = new URL(actionArgs[0]);
                    Path path = Paths.get(actionArgs[1]).toAbsolutePath().normalize();
                    try (InputStream inStream = url.openStream()) {
                        Files.copy(inStream, path, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (Exception e) {
                    System.exit(1);
                }
                break;
            default:
                break;
        }
    }

}
