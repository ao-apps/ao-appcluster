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
 * Contains the results of one resource test.
 *
 * @author  AO Industries, Inc.
 */
public class ResourceTestResult extends ResourceResult {

    private final ResourceStatus resourceStatus;
    private final String output;
    private final String error;

    public ResourceTestResult(
        long startTime,
        long endTime,
        ResourceStatus resourceStatus,
        String output,
        String error
    ) {
        super(startTime, endTime);
        this.resourceStatus = resourceStatus;
        this.output = output;
        this.error = error;
    }

    /**
     * Gets the resource status that this result will cause.
     */
    @Override
    public ResourceStatus getResourceStatus() {
        return resourceStatus;
    }

    /**
     * Gets the output associated with this test or <code>null</code> for none.
     */
    public String getOutput() {
        return output;
    }

    /**
     * Gets the error associated with this test or <code>null</code> for none.
     */
    public String getError() {
        return error;
    }
}
