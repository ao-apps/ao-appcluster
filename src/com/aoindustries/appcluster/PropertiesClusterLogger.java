/*
 * ao-appcluster - Coordinates system components installed in master/slave replication.
 * Copyright (C) 2011  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-appcluster.
 *
 * ao-appcluster is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-appcluster is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-appcluster.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.appcluster;

import java.io.File;

/**
 * Logs the activities of the application cluster to a properties file.
 *
 * @author  AO Industries, Inc.
 */
public class PropertiesClusterLogger implements AppClusterLogger {

    // private static final Logger logger = Logger.getLogger(PropertiesClusterLogger.class.getName());

    private final File propertiesFile;

    public PropertiesClusterLogger(File propertiesFile) {
        // Check that properties file exists, including all the .new, .old stuff.
        this.propertiesFile = propertiesFile;
    }

    @Override
    public String toString() {
        return propertiesFile.getPath();
    }

    @Override
    public void start() {
        // Nothing to do
    }

    @Override
    public void stop() {
        // Nothing to do
    }
}
