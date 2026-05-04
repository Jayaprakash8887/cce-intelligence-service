package org.openphc.cce.intelligence.web.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {
    private T data;
    private PaginationInfo pagination;

    public static <T> ApiResponse<T> of(T data) {
        return ApiResponse.<T>builder().data(data).build();
    }

    public static <T> ApiResponse<T> of(T data, PaginationInfo pagination) {
        return ApiResponse.<T>builder().data(data).pagination(pagination).build();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class PaginationInfo {
        private int page;
        private int size;
        private long totalElements;
        private int totalPages;
    }
}
