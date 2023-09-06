package org.example;

import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void oldMain(String[] args) {
        String path = "db.db";

//        try {
//            DAL dal = new DAL(path);
//
//            Page p = dal.allocateEmptyPage();
//            byte[] dataToCopy = "data".getBytes();
//            System.arraycopy(dataToCopy, 0, p.getData(), 0, dataToCopy.length);
//            dal.writePage(p);
//
//            dal.close();
//
//            dal = new DAL(path);
//
//            p = dal.allocateEmptyPage();
//            byte[] dataToCopy2 = "data2".getBytes();
//            System.arraycopy(dataToCopy2, 0, p.getData(), 0, dataToCopy2.length);
//            dal.writePage(p);
//
//            p = dal.allocateEmptyPage();
//            dal.releasePage(p.getNum());
//
//            dal.close();

            // initialize db
//            DAL dal = new DAL("mainTest");
//
//            Node node = dal.getNode(dal.getRoot());
//            node.setDal(dal);
//
//            Node.Pair<Integer, Node> searchResult = node.findKey("Key1".getBytes());
//            int index = searchResult.key();
//            Node containingNode = searchResult.value();
//
//            Item res = containingNode.getItems().get(index); // Assuming you have a getItems() method that returns the items list
//
//            System.out.printf("key is: %s, value is: %s", new String(res.key()), new String(res.value()));
//
//            // Close the db
//            dal.close();
//        } catch(IOException e) {
//            e.printStackTrace();
//        }
    }

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
        } catch (Exception e) {
//            e.printStackTrace();
            System.out.println(e.getMessage());
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
