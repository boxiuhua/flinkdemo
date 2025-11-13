package com.bigdata.demo4;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.http.annotation.Contract;

@AllArgsConstructor
@Data
public class Student {
    private Integer id;
    private String name;
    private Integer age;

    public Student(String name,Integer age){
        this.name= name;
        this.age=age;
    }
}
