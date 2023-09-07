package org.example;

import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        Options options = new Options();

        DAL dal = null;
        try {
            dal = new DAL("mainTest", options);

            Collection collection = new Collection("collection1".getBytes(), dal.getRoot(), dal);

            collection.put("Key1".getBytes(), "Value1".getBytes());
            collection.put("Key2".getBytes(), "Value2".getBytes());
            collection.put("Key3".getBytes(), "Value3".getBytes());
            collection.put("Key4".getBytes(), "Value4".getBytes());
            collection.put("Key5".getBytes(), "Value5".getBytes());
            collection.put("Key6".getBytes(), "Value6".getBytes());
            Item item = collection.find("Key3".getBytes());

            System.out.printf("key is: %s, value is: %s\n", new String(item.key()), new String(item.value()));

            collection.remove("Key1".getBytes());
            item = collection.find("Key1".getBytes());
            System.out.printf("Item is: %s\n", item);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dal != null) {
                try {
                    dal.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
