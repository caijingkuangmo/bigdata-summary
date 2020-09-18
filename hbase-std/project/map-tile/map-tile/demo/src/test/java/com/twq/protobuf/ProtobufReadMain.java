package com.twq.protobuf;

import com.twq.tutorial.AddressBookProtos;

import java.io.FileInputStream;
import java.io.IOException;

public class ProtobufReadMain {
    public static void main(String[] args) throws IOException {
        AddressBookProtos.AddressBook addressBook =
                AddressBookProtos.AddressBook.parseFrom(new FileInputStream("address_book_proto.txt"));

        //byte[] aByte = addressBook.toByteArray();

        print(addressBook);
    }

    static void print(AddressBookProtos.AddressBook addressBook) {
        for (AddressBookProtos.Person person : addressBook.getPeopleList()) {
            System.out.println("Person ID: " + person.getId());
            System.out.println("  Name: " + person.getName());
            if (person.hasEmail()) {
                System.out.println("  E-mail address: " + person.getEmail());
            }

            for (AddressBookProtos.Person.PhoneNumber phoneNumber : person.getPhonesList()) {
                switch (phoneNumber.getType()) {
                    case MOBILE:
                        System.out.print("  Mobile phone #: ");
                        break;
                    case HOME:
                        System.out.print("  Home phone #: ");
                        break;
                    case WORK:
                        System.out.print("  Work phone #: ");
                        break;
                }
                System.out.println(phoneNumber.getNumber());
            }
        }
    }
}
