public class Test {
    public static void main(String[] args) {
        String str = "2019-07-31 13:00-17:00";
        String[] fields = str.split(" ");
        String preStr = fields[0];
        String[] fields2 = fields[1].split("-");
        System.out.println(preStr+" "+fields2[0]);
        System.out.println(preStr+" "+fields2[1]);
    }
}
