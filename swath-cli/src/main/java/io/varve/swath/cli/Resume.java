/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Classifies a visible {@code @Option} for checkpoint resume, sitting on the <i>same</i> program
 * element as the {@code @Option} so the annotation can never name a wrong option.
 * {@link ResumeRegistryDriftTest} asserts every visible option carries one.
 *
 * <p>{@link #restored()} models a fourth-class distinction beyond the three {@link ResumeClass}
 * values: a
 * destination path ({@code -o}) or {@code --format} is {@code IDENTITY} yet must be <i>restored</i>
 * on a bare resume (that is how a bare resume learns where and how to write) <i>and</i> refused when
 * re-passed differently — {@code @Resume(value = IDENTITY, restored = true)} rather than a fourth
 * enum constant.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
@interface Resume {
    ResumeClass value();

    boolean restored() default false;
}
