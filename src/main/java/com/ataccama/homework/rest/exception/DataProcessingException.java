package com.ataccama.homework.rest.exception;

import org.json.JSONObject;

public class DataProcessingException extends RuntimeException {

    public DataProcessingException() {
        super("There was a problem during data processing. Check log for more information.");
    }

    /**
     * @return Json formatted string that contains error message.
     */
    @Override
    public String getMessage() {
        return new JSONObject()
                .put("msg", super.getMessage())
                .toString();
    }

}
