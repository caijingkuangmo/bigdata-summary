package com.twq.protobuf.java;

import java.io.Serializable;

public class PhoneNumber implements Serializable {
    private String number;
    private PhoneType phoneType = PhoneType.HOME;
    enum PhoneType {
        MOBILE,
        HOME,
        WORK
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public PhoneType getPhoneType() {
        return phoneType;
    }

    public void setPhoneType(PhoneType phoneType) {
        this.phoneType = phoneType;
    }

    @Override
    public String toString() {
        return "PhoneNumber{" +
                "number='" + number + '\'' +
                ", phoneType=" + phoneType +
                '}';
    }
}
