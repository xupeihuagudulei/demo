package com.jsy.thread;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * @Author: jsy
 * @Date: 2021/5/23 18:03
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Task1 implements Runnable {

    private String arg1;

    @Override
    public void run() {
        handle();
    }

    private void handle() {
        if (StringUtils.isBlank(arg1)) {
            System.out.println("schedule task running");
        }else {
            System.out.println("task 1 is running");
            System.out.println("arg1 = " + arg1);
        }

    }
}
