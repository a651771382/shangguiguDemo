public class TestOperator {
    public static void main(String[] args) {
        String s1 = "Hello";
        String s2 = new String("Hello");

        Boolean isEqual = s1 == s2;
        System.out.println(isEqual);
        System.out.println(s1.equals(s2));

        System.out.println("======================");
        //赋值运算符
        byte b = 10;
        b = (byte) (b + 1);
        b += 1; // 默认会做强转
        System.out.println(b);

        // 自增自减
        int x = 15;
        int y = x++;
        System.out.println("x=:" + x + ",y=:" + y);

        x = 15;
        y = x++;
        System.out.println("x=:" + x + ",y=:" + y);

        x = 23;
        x = x++;
        System.out.println(x);
    }
}
