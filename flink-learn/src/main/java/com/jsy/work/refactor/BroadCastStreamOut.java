package com.jsy.work.refactor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Map;

/**
 * @Author: jsy
 * @Date: 2021/8/8 1:50
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BroadCastStreamOut {

    private Map<String, Tuple2<String, Integer>> broadcast;

    private UserBehavior userBehavior;

}
