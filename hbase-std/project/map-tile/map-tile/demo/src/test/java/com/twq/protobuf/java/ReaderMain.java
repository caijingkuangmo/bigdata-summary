package com.twq.protobuf.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ReaderMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream("address_book.txt"));
        AddressBook addressBook = (AddressBook) objectInputStream.readObject();
        System.out.println(addressBook);
        objectInputStream.close();
    }
}
