/**
 * Copyright (C), 2015-2019, 学习
 * FileName: Assert
 * Author:   stg05
 * Date:     2019/5/16 10:59
 * Description: 自定义复杂数据类型
 * History:
 */
package com.hadoop.study.mapreduce.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 〈自定义复杂数据类型〉
 * 1) 按照hadoop规范  实现 Writable接口
 * 2）按照hadoop规范   重写方法
 * 3）默认构造方法
 */
public class Access implements Writable {

    private String phone;

    private long up;

    private long down;

    private long sum;

    /*
            按照顺序读写，先读先写
     */

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public Access(){}

    public Access(String phone,long up,long down){
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    @Override
    public String toString() {
        return  phone  +
                "," + up +
                "," + down +
                "," + sum;
    }
}
 
