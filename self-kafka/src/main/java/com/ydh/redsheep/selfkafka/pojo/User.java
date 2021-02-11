package com.ydh.redsheep.selfkafka.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户自定义的封装消息的实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {

    private Integer userId;
    private String username;

}
