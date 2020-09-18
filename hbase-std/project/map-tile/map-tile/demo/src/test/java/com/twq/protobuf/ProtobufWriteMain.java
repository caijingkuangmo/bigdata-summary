package com.twq.protobuf;

import com.twq.tutorial.AddressBookProtos;

import java.io.FileOutputStream;
import java.io.IOException;

public class ProtobufWriteMain {
    public static void main(String[] args) throws IOException {
        AddressBookProtos.Person john =
                AddressBookProtos.Person.newBuilder()
                        .setId(1234)
                        .setName("John Doe")
                        .setEmail("jdoe@example.com")
                        .addPhones(
                                AddressBookProtos.Person.PhoneNumber.newBuilder()
                                        .setNumber("555-4321")
                                        .setType(AddressBookProtos.Person.PhoneType.HOME))
                        .build();
        AddressBookProtos.AddressBook.Builder addressBook = AddressBookProtos.AddressBook.newBuilder();
        addressBook.addPeople(john);

        //byte[] aByte = addressBook.build().toByteArray();

        FileOutputStream output = new FileOutputStream("address_book_proto.txt");
        addressBook.build().writeTo(output);
        output.close();
    }
}
