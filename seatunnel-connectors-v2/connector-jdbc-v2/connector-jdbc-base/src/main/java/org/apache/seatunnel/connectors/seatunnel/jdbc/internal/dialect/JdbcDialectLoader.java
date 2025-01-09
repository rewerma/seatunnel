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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/** Utility for working with {@link JdbcDialect}. */
public final class JdbcDialectLoader {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcDialectLoader.class);

    private static final ConcurrentMap<String, JdbcDialect> dialectCache =
            new ConcurrentHashMap<>();

    private static final String JDBC_URL_PREFIX = "jdbc:";

    private JdbcDialectLoader() {}

    public static JdbcDialect load(String dbType, String compatibleMode) {
        return load(dbType, compatibleMode, "");
    }

    /**
     * Loads the unique JDBC Dialect that can handle the given database url.
     *
     * @param dbTypeOrUrl The database type or jdbc url.
     * @param compatibleMode The compatible mode.
     * @return The loaded dialect.
     * @throws IllegalStateException if the loader cannot find exactly one dialect that can
     *     unambiguously process the given database URL.
     */
    public static JdbcDialect load(String dbTypeOrUrl, String compatibleMode, String fieldIde) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<JdbcDialectFactory> foundFactories = discoverFactories(cl);

        if (foundFactories.isEmpty()) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DIALECT_FACTORY,
                    String.format(
                            "Could not find any jdbc dialect factories that implement '%s' in the classpath.",
                            JdbcDialectFactory.class.getName()));
        }

        return dialectCache.computeIfAbsent(
                dbTypeOrUrl.toLowerCase(),
                v -> {
                    List<JdbcDialectFactory> matchingFactories =
                            foundFactories.stream()
                                    .filter(
                                            f -> {
                                                if (dbTypeOrUrl.startsWith(JDBC_URL_PREFIX)) {
                                                    return f.acceptsURL(dbTypeOrUrl);
                                                } else {
                                                    return f.dialectIdentifier()
                                                                    .equalsIgnoreCase(dbTypeOrUrl)
                                                            || f.dialectIdentifier()
                                                                    .equalsIgnoreCase(
                                                                            GenericDialectFactory
                                                                                    .DIALECT_NAME);
                                                }
                                            })
                                    .collect(Collectors.toList());

                    // filter out generic dialect factory
                    if (matchingFactories.size() > 1) {
                        matchingFactories =
                                matchingFactories.stream()
                                        .filter(f -> !(f instanceof GenericDialectFactory))
                                        .collect(Collectors.toList());
                    }

                    if (matchingFactories.size() > 1) {
                        throw new JdbcConnectorException(
                                JdbcConnectorErrorCode.NO_SUITABLE_DIALECT_FACTORY,
                                String.format(
                                        "Multiple jdbc dialect factories can handle dbType '%s' that implement '%s' found in the classpath.\n\n"
                                                + "Ambiguous factory classes are:\n\n"
                                                + "%s",
                                        dbTypeOrUrl,
                                        JdbcDialectFactory.class.getName(),
                                        matchingFactories.stream()
                                                .map(f -> f.getClass().getName())
                                                .sorted()
                                                .collect(Collectors.joining("\n"))));
                    }

                    return matchingFactories.get(0).create(compatibleMode, fieldIde);
                });
    }

    private static List<JdbcDialectFactory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<JdbcDialectFactory> result = new LinkedList<>();
            ServiceLoader.load(JdbcDialectFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            LOG.error("Could not load service provider for jdbc dialects factory.", e);
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DIALECT_FACTORY,
                    "Could not load service provider for jdbc dialects factory.",
                    e);
        }
    }
}
