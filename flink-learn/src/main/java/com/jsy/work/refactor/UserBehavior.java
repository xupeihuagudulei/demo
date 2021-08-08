package com.jsy.work.refactor;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: jsy
 * @Date: 2021/7/30 6:24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("item_id")
    private String itemId;

    @JsonProperty("category_id")
    private String categoryId;

    private String behavior;
    private String ts;
}
