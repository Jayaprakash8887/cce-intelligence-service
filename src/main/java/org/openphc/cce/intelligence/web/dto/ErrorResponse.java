package org.openphc.cce.intelligence.web.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorResponse {
    private ErrorBody error;

    public static ErrorResponse of(String code, String message, String path) {
        return ErrorResponse.builder()
                .error(ErrorBody.builder()
                        .code(code)
                        .message(message)
                        .path(path)
                        .timestamp(OffsetDateTime.now())
                        .build())
                .build();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ErrorBody {
        private String code;
        private String message;
        private String path;
        private OffsetDateTime timestamp;
        private List<FieldError> fieldErrors;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class FieldError {
        private String field;
        private String message;
    }
}
