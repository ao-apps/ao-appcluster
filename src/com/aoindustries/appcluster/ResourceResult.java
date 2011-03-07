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

import java.sql.Timestamp;

/**
 * Parent class for information common to different types of resource results.
 *
 * @author  AO Industries, Inc.
 */
abstract public class ResourceResult {

    protected final long startTime;
    protected final long endTime;

    ResourceResult(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public Timestamp getStartTime() {
        return new Timestamp(startTime);
    }

    public Timestamp getEndTime() {
        return new Timestamp(endTime);
    }

    /**
     * Gets the ResourceStatus this result will cause.
     */
    abstract public ResourceStatus getResourceStatus();
}