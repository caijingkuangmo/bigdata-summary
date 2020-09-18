package com.twq.protobuf.java;

import java.io.Serializable;
import java.util.List;

public class AddressBook implements Serializable {

    private List<Person> personList;

    public List<Person> getPersonList() {
        return personList;
    }

    public void setPersonList(List<Person> personList) {
        this.personList = personList;
    }

    @Override
    public String toString() {
        return "AddressBook{" +
                "personList=" + personList +
                '}';
    }

}
