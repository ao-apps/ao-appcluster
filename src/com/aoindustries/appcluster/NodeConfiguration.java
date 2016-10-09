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

import java.util.Set;
import org.xbill.DNS.Name;

/**
 * The configuration for a node.
 *
 * @author  AO Industries, Inc.
 */
public interface NodeConfiguration {

	@Override
	String toString();

	@Override
	boolean equals(Object o);

	@Override
	int hashCode();

	/**
	 * @see Node#getId()
	 */
	String getId();

	/**
	 * @see Node#isEnabled()
	 */
	boolean isEnabled();

	/**
	 * @see Node#getDisplay()
	 */
	String getDisplay();

	/**
	 * @see Node#getHostname()
	 */
	Name getHostname();

	/**
	 * @see Node#getUsername()
	 */
	String getUsername();

	/**
	 * @see Node#getNameservers()
	 */
	Set<? extends Name> getNameservers();
}
