package com.jsy.thread;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: jsy
 * @Date: 2021/5/23 18:23
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person implements Comparable<Person> {

    private Integer id;

    private String name;

    @Override
    public int compareTo(Person person) {
        return this.id > person.getId() ? 1 : (this.id < person.getId() ? -1 : 0);

    }
}
