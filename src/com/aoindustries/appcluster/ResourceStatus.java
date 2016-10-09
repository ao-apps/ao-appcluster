/*
 * ao-appcluster - Application-level clustering tools.
 * Copyright (C) 2011, 2016  AO Industries, Inc.
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

import java.util.logging.Level;

/**
 * The overall status of a resource is based on its master records and its nodes.
 *
 * @see Resource#getStatus()
 *
 * @author  AO Industries, Inc.
 */
public enum ResourceStatus {
	// These are arranged from healthies to least healthy, each higher will override the previous.
	UNKNOWN("background-color:#606060;", Level.FINE),
	DISABLED("background-color:#808080;", Level.FINE),
	STOPPED("background-color:#c0c0c0;", Level.FINE),
	HEALTHY("", Level.INFO),
	STARTING("background-color:#e0e0e0;", Level.INFO),
	WARNING("background-color:#ff8080;", Level.WARNING),
	ERROR("background-color:#ff4040;", Level.SEVERE),
	INCONSISTENT("background-color:#ff0000;", Level.SEVERE);

	private final String cssStyle;
	private final Level logLevel;

	private ResourceStatus(String cssStyle, Level logLevel) {
		this.cssStyle = cssStyle;
		this.logLevel = logLevel;
	}

	@Override
	public String toString() {
		return ApplicationResources.accessor.getMessage("ResourceStatus." + name());
	}

	/**
	 * JavaBeans compatibility.
	 */
	public String getName() {
		return name();
	}

	/**
	 * Gets the CSS style to use for this status or "" for no specific style requirement.
	 */
	public String getCssStyle() {
		return cssStyle;
	}

	/**
	 * Gets the log level recommended for messages associated with this status.
	 */
	public Level getLogLevel() {
		return logLevel;
	}
}
