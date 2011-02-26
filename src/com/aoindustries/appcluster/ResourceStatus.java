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
 * The overall status of a resource is based on its master records and its nodes.
 *
 * @see Resource#getStatus()
 *
 * @author  AO Industries, Inc.
 */
public enum ResourceStatus {
    // These are arranged from healthies to least healthy, each higher will override the previous.
    UNKNOWN("background-color:#606060;"),
    DISABLED("background-color:#808080;"),
    STOPPED("background-color:#c0c0c0;"),
    STARTING("background-color:#ff8000;"),
    HEALTHY(""),
    WARNING("background-color:#ff0080;"),
    ERROR("background-color:#ff8080;"),
    INCONSISTENT("background-color:#ff0000;");

    private final String cssStyle;

    private ResourceStatus(String cssStyle) {
        this.cssStyle = cssStyle;
    }

    @Override
    public String toString() {
        return ApplicationResources.accessor.getMessage("ResourceStatus." + name());
    }

    /**
     * Gets the CSS style to use for this status or "" for no specific style requirement.
     */
    public String getCssStyle() {
        return cssStyle;
    }
}
