package com.yw.flink.example;

public class PersonInfo {
    private String phoneNum;
    private String name;
    private String city;

    public PersonInfo(){

    }

    public PersonInfo(String phoneNum, String name, String city) {
        this.phoneNum = phoneNum;
        this.name = name;
        this.city = city;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public String getName() {
        return name;
    }

    public String getCity() {
        return city;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "PersonInfo{" +
                "phoneNum='" + phoneNum + '\'' +
                ", name='" + name + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
