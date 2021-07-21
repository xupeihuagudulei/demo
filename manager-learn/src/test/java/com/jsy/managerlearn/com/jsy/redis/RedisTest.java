package com.jsy.managerlearn.com.jsy.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisTest {

    @Autowired
    RedisTemplate redisTemplate;

    // 放的是string，避免key使用linux黑窗口查看时前面有特殊字符
    @Autowired
    StringRedisTemplate stringRedisTemplate;

    @Test
    public void test1() {
        /*
         * 键相关的指令
         * */
        Set keys = redisTemplate.keys("*");
    }

    // 存数据
    @Test
    public void testSet() {
        /*
         * string   redisTemplate.opsForValue().set("name","zhangsan");
         * list     redisTemplate.opsForList().leftPush("list","a");
         * set      redisTemplate.opsForSet().add("set","a","b","c");
         * zset     redisTemplate.opsForZSet().add("zset","aa",20);
         * hash     redisTemplate.opsForHash().put("map","id","1");    putAll   可以放多个
         * */
        stringRedisTemplate.opsForValue().set("string", "zhangsan");
        stringRedisTemplate.opsForList().leftPush("list", "aa");
        stringRedisTemplate.opsForSet().add("set", "a", "b", "c");
        stringRedisTemplate.opsForZSet().add("zset", "aa", 20);
        stringRedisTemplate.opsForHash().put("map", "id", "1");
    }

    // 取数据
    @Test
    public void testGet() {
        String str = stringRedisTemplate.opsForValue().get("string");
        System.out.println(str);
        List<String> list = stringRedisTemplate.opsForList().range("list", 0, -1);
        for (String s : list) {
            System.out.println(s);
        }

        Set<String> set = stringRedisTemplate.opsForSet().members("set");
        for (String s : set) {
            System.out.println(s);
        }
        Set<ZSetOperations.TypedTuple<String>> zset = stringRedisTemplate.opsForZSet().rangeWithScores("zset", 0, -1);
        for (ZSetOperations.TypedTuple<String> stringTypedTuple : zset) {
            System.out.println(stringTypedTuple.getValue());
            System.out.println(stringTypedTuple.getScore());
        }
        Map<Object, Object> map = stringRedisTemplate.opsForHash().entries("map");
        for (Object o : map.keySet()) {
            System.out.println(o);
            System.out.println(map.get(o));
        }
    }

    @Test
    // 推荐使用绑定方法，不用每次都写键（key）了
    public void name() {

        // 参数绑定，把key做一个绑定  绑定完以后是一个value的Ops   就可以对key1进行操作
        BoundValueOperations<String, String> boundvalueOps = stringRedisTemplate.boundValueOps("key1");
        boundvalueOps.set("value1");
        String s = boundvalueOps.get();
        System.out.println(s);
        // hash是大键
        BoundHashOperations<String, Object, Object> boundHashOperations = stringRedisTemplate.boundHashOps("hash");
        // id 是小键，大键绑定了，就不用每次都写大键了
        boundHashOperations.put("id", "1");
        boundHashOperations.put("name", "zhangsan");
        Map<Object, Object> entries = boundHashOperations.entries();
        for (Object o : entries.keySet()) {
            System.out.println(o);
            System.out.println(entries.get(o));
        }
    }

}
