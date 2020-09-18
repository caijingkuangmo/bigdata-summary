package com.twq.protobuf.java;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        List<Person> personList = new ArrayList<>();
        Person person1 = new Person();
        person1.setName("jeffy");
        person1.setId(1);
        person1.setEmailAddress("2343@qq.com");
        PhoneNumber phoneNumber1 = new PhoneNumber();
        phoneNumber1.setNumber("16721342739");
        phoneNumber1.setPhoneType(PhoneNumber.PhoneType.MOBILE);
        person1.setPhoneNumbers(Arrays.asList(phoneNumber1));
        personList.add(person1);

        Person person2 = new Person();
        person2.setName("jeffy-2");
        person2.setId(2);
        person2.setEmailAddress("23433333@qq.com");
        PhoneNumber phoneNumber2 = new PhoneNumber();
        phoneNumber2.setNumber("2342345");
        phoneNumber2.setPhoneType(PhoneNumber.PhoneType.HOME);
        person2.setPhoneNumbers(Arrays.asList(phoneNumber1));
        personList.add(person2);

        AddressBook addressBook = new AddressBook();
        addressBook.setPersonList(personList);

        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("address_book.txt"));
        objectOutputStream.writeObject(addressBook);
        objectOutputStream.close();
    }
}
