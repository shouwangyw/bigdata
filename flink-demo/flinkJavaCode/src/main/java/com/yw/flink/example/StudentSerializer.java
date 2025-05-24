package com.yw.flink.example;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author yangwei
 */
public class StudentSerializer extends Serializer {
    @Override
    public void write(Kryo kryo, Output output, Object o) {
        Student student = (Student) o;
        output.writeInt(student.getId());
        output.writeString(student.getName());
        output.writeInt(student.getAge());
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        return new Student(input.readInt(), input.readString(), input.readInt());
    }
}
