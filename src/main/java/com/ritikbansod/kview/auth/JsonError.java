package com.ritikbansod.kview.auth;

import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

/** JSON error bodies in the API's {@code {error, detail}} shape, for filter-level rejections. */
final class JsonError {

    private JsonError() { }

    static void write(HttpServletResponse response, int status, String error, String detail) throws IOException {
        response.setStatus(status);
        response.setHeader("WWW-Authenticate", "Bearer");
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write("{\"error\":\"" + error + "\",\"detail\":\"" + detail + "\"}");
    }
}
