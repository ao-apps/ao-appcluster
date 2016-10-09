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

/**
 * The possible states for resource synchronization.
 *
 * @see  Resource
 * 
 * @author  AO Industries, Inc.
 */
public enum ResourceSynchronizerState {
	DISABLED(ResourceStatus.DISABLED, ResourceStatus.DISABLED.getCssStyle()),
	STOPPED(ResourceStatus.STOPPED, ResourceStatus.STOPPED.getCssStyle()),
	SLEEPING(ResourceStatus.HEALTHY, ResourceStatus.HEALTHY.getCssStyle()),
	TESTING(ResourceStatus.HEALTHY, ResourceStatus.HEALTHY.getCssStyle() + "background-color:#00ff00;"),
	SYNCHRONIZING(ResourceStatus.HEALTHY, ResourceStatus.HEALTHY.getCssStyle() + "background-color:#8080ff;");

	private final ResourceStatus resourceStatus;
	private final String cssStyle;

	private ResourceSynchronizerState(ResourceStatus resourceStatus, String cssStyle) {
		this.resourceStatus = resourceStatus;
		this.cssStyle = cssStyle;
	}

	@Override
	public String toString() {
		return ApplicationResources.accessor.getMessage("ResourceSynchronizerState." + name());
	}

	/**
	 * Gets the resource status that this synchronization status will cause.
	 */
	public ResourceStatus getResourceStatus() {
		return resourceStatus;
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
}
