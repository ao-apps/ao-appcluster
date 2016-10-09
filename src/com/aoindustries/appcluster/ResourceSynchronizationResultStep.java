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

import com.aoindustries.util.AoCollections;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * One step of the results of a resource synchronization (or test).
 *
 * @author  AO Industries, Inc.
 */
public class ResourceSynchronizationResultStep implements ResourceResult {

	final long startTime;
	final long endTime;
	private final ResourceStatus resourceStatus;
	private final String description;
	private final List<String> outputs;
	private final List<String> warnings;
	private final List<String> errors;

	private static List<String> asUnmodifiableList(CharSequence chars) {
		if(chars==null || chars.length()==0) return Collections.emptyList();
		return Collections.singletonList(chars.toString());
	}

	/**
	 * @param description May not be <code>null</code>
	 * @param output <code>null</code> or empty is converted to empty list
	 * @param warning <code>null</code> or empty is converted to empty list
	 * @param error <code>null</code> or empty is converted to empty list
	 */
	public ResourceSynchronizationResultStep(
		long startTime,
		long endTime,
		ResourceStatus resourceStatus,
		String description,
		CharSequence output,
		CharSequence warning,
		CharSequence error
	) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.resourceStatus = resourceStatus;
		if(description==null) throw new IllegalArgumentException("description==null");
		this.description = description;
		this.outputs = asUnmodifiableList(output);
		this.warnings = asUnmodifiableList(warning);
		this.errors = asUnmodifiableList(error);
	}

	/**
	 * @param description May not be <code>null</code>
	 * @param outputs <code>null</code> is converted to empty list and a defensive copy is made
	 * @param warnings <code>null</code> is converted to empty list and a defensive copy is made
	 * @param errors <code>null</code> is converted to empty list and a defensive copy is made
	 */
	public ResourceSynchronizationResultStep(
		long startTime,
		long endTime,
		ResourceStatus resourceStatus,
		String description,
		Collection<String> outputs,
		Collection<String> warnings,
		Collection<String> errors
	) {
		this.startTime = startTime;
		this.endTime = endTime;
		this.resourceStatus = resourceStatus;
		if(description==null) throw new IllegalArgumentException("description==null");
		this.description = description;
		if(outputs==null) this.outputs = Collections.emptyList();
		else this.outputs = AoCollections.unmodifiableCopyList(outputs);
		if(warnings==null) this.warnings = Collections.emptyList();
		else this.warnings = AoCollections.unmodifiableCopyList(warnings);
		if(errors==null) this.errors = Collections.emptyList();
		else this.errors = AoCollections.unmodifiableCopyList(errors);
	}

	@Override
	public Timestamp getStartTime() {
		return new Timestamp(startTime);
	}

	@Override
	public Timestamp getEndTime() {
		return new Timestamp(endTime);
	}

	/**
	 * Gets the resource status that this result will cause.
	 */
	@Override
	public ResourceStatus getResourceStatus() {
		return resourceStatus;
	}

	/**
	 * Gets the description for this step.
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Gets the outputs associated with this test or empty list for none.
	 */
	public List<String> getOutputs() {
		return outputs;
	}

	/**
	 * Gets the warnings associated with this test or empty list for none.
	 */
	public List<String> getWarnings() {
		return warnings;
	}

	/**
	 * Gets the errors associated with this test or empty list for none.
	 */
	public List<String> getErrors() {
		return errors;
	}
}
