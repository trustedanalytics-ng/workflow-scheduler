/**
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.scheduler;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.trustedanalytics.utils.errorhandling.ErrorLogger;
import org.trustedanalytics.utils.errorhandling.RestErrorHandler;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

@ControllerAdvice
public class WorkflowSchedulerExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowSchedulerExceptionHandler.class);

    @ExceptionHandler(AccessDeniedException.class)
    public void handleNoPermissionToOrganizationException(AccessDeniedException e, HttpServletResponse response) throws Exception {
        ErrorLogger.logAndSendErrorResponse(LOGGER, response, FORBIDDEN, e.getMessage(), e);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public void handleIllegalArgumentException(IllegalArgumentException e, HttpServletResponse response) throws Exception {
        ErrorLogger.logAndSendErrorResponse(LOGGER, response, BAD_REQUEST, e.getMessage(), e);
    }

    @ExceptionHandler(IllegalStateException.class)
    public void handleIllegalStateException(IllegalStateException e, HttpServletResponse response) throws Exception {
        ErrorLogger.logAndSendErrorResponse(LOGGER, response, INTERNAL_SERVER_ERROR, e.getMessage(), e);
    }

    @ExceptionHandler(Exception.class)
    public void handleException(Exception e, HttpServletResponse response) throws Exception {
        RestErrorHandler defaultErrorHandler = new RestErrorHandler();
        defaultErrorHandler.handleException(e, response);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public void handleMethodArgumentNotValidException(MethodArgumentNotValidException e, HttpServletResponse response) throws Exception {
        BindingResult result = e.getBindingResult();
        List<FieldError> fieldErrors = result.getFieldErrors();
        ErrorLogger.logAndSendErrorResponse(LOGGER, response, BAD_REQUEST, processFieldErrors(fieldErrors), e);
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public void handleMissingParams(MissingServletRequestParameterException e, HttpServletResponse response) throws IOException {
        LOGGER.error("Missing parameter: {}", e.getParameterName());
        ErrorLogger.logAndSendErrorResponse(LOGGER, response, BAD_REQUEST, e.getMessage(), e);
    }

    private String processFieldErrors(List<FieldError> fieldErrors) {
        return fieldErrors.stream()
                .map(FieldError::getDefaultMessage)
                .reduce("Validation errors: ", (m1, m2) -> m1 + m2 + "; ");
    }
}
