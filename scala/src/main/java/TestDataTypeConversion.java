public class TestDataTypeConversion {
    public static void main(String[] args) {
        byte b = 10;
        test(b);
        char c = 20;
        short c2 = (short) c;
        test(c);
    }

//    public static void test(byte b) {
//        System.out.println("bbbb");
//    }

    public static void test(short c) {
        System.out.println("cccc");
    }

    public static void test(char s) {
        System.out.println("ssss");
    }

    public static void test(int r) {
        System.out.println("rrrr");
    }
}
