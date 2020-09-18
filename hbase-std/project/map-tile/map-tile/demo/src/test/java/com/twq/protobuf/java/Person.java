package com.twq.protobuf.java;

import java.io.Serializable;
import java.util.List;

public class Person implements Serializable {
    private String name;
    private int id;
    private String emailAddress;
    private List<PhoneNumber> phoneNumbers;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public List<PhoneNumber> getPhoneNumbers() {
        return phoneNumbers;
    }

    public void setPhoneNumbers(List<PhoneNumber> phoneNumbers) {
        this.phoneNumbers = phoneNumbers;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", id=" + id +
                ", emailAddress='" + emailAddress + '\'' +
                ", phoneNumbers=" + phoneNumbers +
                '}';
    }
}
