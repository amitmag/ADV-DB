public class Main {
    public static void main(String[] args) {
        Assigment a1 = new Assigment("jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle",
                                        "amitmag", "abcd");
        a1.fileToDataBase("D:\\documents\\users\\amitmag\\Downloads\\films.csv");
        a1.calculateSimilarity();
        a1.printSimilarItems(1);
        a1.Disconnect();
    }
}
