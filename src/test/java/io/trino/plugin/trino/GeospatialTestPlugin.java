/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.trino;

import io.trino.spi.Plugin;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Trino publishes the geospatial plugin as a release ZIP, not as a Maven JAR.
 * Load the official 481 plugin into the in-process test runners without adding
 * a geospatial dependency to the production connector.
 */
final class GeospatialTestPlugin
{
    private static final String RELEASE_URL = "https://github.com/trinodb/trino/releases/download/481/trino-geospatial-481.zip";
    private static final Path TEST_DIRECTORY = Path.of("target", "geospatial-test-plugin-481");
    private static URLClassLoader classLoader;

    private GeospatialTestPlugin() {}

    static synchronized Plugin load()
    {
        try {
            if (classLoader == null) {
                Path pluginDirectory = preparePluginDirectory();
                List<URL> urls;
                try (var jars = Files.list(pluginDirectory)) {
                    urls = jars.filter(path -> path.getFileName().toString().endsWith(".jar"))
                            .sorted()
                            .map(GeospatialTestPlugin::toUrl)
                            .toList();
                }
                classLoader = new URLClassLoader(urls.toArray(URL[]::new), GeospatialTestPlugin.class.getClassLoader());
            }
            return (Plugin) classLoader.loadClass("io.trino.plugin.geospatial.GeoPlugin")
                    .getConstructor()
                    .newInstance();
        }
        catch (ReflectiveOperationException | IOException e) {
            throw new IllegalStateException("Unable to load the official Trino 481 geospatial test plugin", e);
        }
    }

    private static Path preparePluginDirectory()
            throws IOException
    {
        Files.createDirectories(TEST_DIRECTORY);
        Path pluginJar = TEST_DIRECTORY.resolve("io.trino_trino-geospatial-481.jar");
        Path extractionMarker = TEST_DIRECTORY.resolve(".complete");
        if (Files.exists(pluginJar) && Files.exists(extractionMarker)) {
            return TEST_DIRECTORY;
        }

        Path archive = Path.of("target", "trino-geospatial-481.zip");
        if (!Files.exists(archive)) {
            String configuredArchive = System.getenv("TRINO_GEOSPATIAL_PLUGIN_ZIP");
            if (configuredArchive != null && !configuredArchive.isBlank()) {
                archive = Path.of(configuredArchive);
            }
            else {
                Files.createDirectories(archive.getParent());
                Path download = archive.resolveSibling("trino-geospatial-481.zip.download");
                HttpURLConnection connection = (HttpURLConnection) URI.create(RELEASE_URL).toURL().openConnection();
                connection.setConnectTimeout(30_000);
                connection.setReadTimeout(120_000);
                try {
                    try (var input = connection.getInputStream()) {
                        Files.copy(input, download, REPLACE_EXISTING);
                    }
                    Files.move(download, archive, REPLACE_EXISTING);
                }
                finally {
                    Files.deleteIfExists(download);
                }
            }
        }

        try (ZipFile zip = new ZipFile(archive.toFile())) {
            var entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (entry.isDirectory() || !entry.getName().endsWith(".jar")) {
                    continue;
                }
                Path target = TEST_DIRECTORY.resolve(Path.of(entry.getName()).getFileName().toString());
                try (var input = zip.getInputStream(entry)) {
                    Files.copy(input, target, REPLACE_EXISTING);
                }
            }
        }
        if (!Files.exists(pluginJar)) {
            throw new IOException("Trino 481 geospatial release ZIP does not contain its plugin JAR");
        }
        Files.writeString(extractionMarker, "481");
        return TEST_DIRECTORY;
    }

    private static URL toUrl(Path path)
    {
        try {
            return path.toUri().toURL();
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to resolve geospatial test plugin JAR: " + path, e);
        }
    }
}
