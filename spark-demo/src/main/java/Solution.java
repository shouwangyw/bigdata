import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yangwei
 */
public class Solution {

    public static void main(String[] args) {

    }




//    public static void main(String[] args) {
//        List<TableA> tableA = Arrays.asList(
//                new TableA(1, "Alice"),
//                new TableA(2, "Bob"),
//                new TableA(3, "Cyc")
//        );
//        List<TableB> tableB = Arrays.asList(
//                new TableB(1, 23),
//                new TableB(2, 34),
//                new TableB(4, 18)
//        );
//        List<Table> joinedList = tableA.stream().flatMap(
//                a -> tableB.stream()
//                        .filter(b -> a.getId() == b.getId())
//                        .map(b -> new Table(a.id, a.name, b.age))
//        ).collect(Collectors.toList());
//
//        System.out.println(joinedList);
//    }
//
//    @Data
//    @AllArgsConstructor
//    public static class TableA {
//        int id;
//        String name;
//    }
//
//    @Data
//    @AllArgsConstructor
//    public static class TableB {
//        int id;
//        int age;
//    }
//
//    @Data
//    @AllArgsConstructor
//    @ToString
//    public static class Table {
//        int id;
//        String name;
//        int age;
//    }
}