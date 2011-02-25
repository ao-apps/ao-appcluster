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

/**
 * The overall cluster is that of its most unhealthy resource.
 *
 * @see AppCluster#getStatus()
 *
 * @author  AO Industries, Inc.
 */
public enum AppClusterStatus {
    STOPPED("#808080"),
    DISABLED("#c0c0c0"),
    UNKNOWN("#ff0000"),
    WARNING("#ff0080"),
    ERROR("#ff0000"),
    INCONSISTENT("#ff0000"),
    HEALTHY("transparent");

    private final String cssColor;

    private AppClusterStatus(String cssColor) {
        this.cssColor = cssColor;
    }

    @Override
    public String toString() {
        return ApplicationResources.accessor.getMessage("AppClusterStatus." + name());
    }

    public String getcssColor() {
        return cssColor;
    }
}
