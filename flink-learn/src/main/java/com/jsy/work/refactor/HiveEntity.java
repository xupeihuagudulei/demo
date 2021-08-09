package com.jsy.work.refactor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: jsy
 * @Date: 2021/8/9 8:17
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HiveEntity {
    private String userId;
    private Integer metric;
    private String ptD;

}
