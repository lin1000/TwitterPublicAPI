package demo.lin1000.utility;

@FunctionalInterface
interface CheckedFunction<T, R, EX extends Exception> {
 
    R apply(T element) throws EX;
 
}