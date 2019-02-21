package demo.lin1000.utility;

import com.google.common.base.Throwables;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Stream;

public class FileUtil{

    /**
     * Handled by google guava Throwables.propagate(e)
     */

    public static Stream<Path> filesInDir(Path dir)  {
        return listFiles(dir)
                .flatMap(path -> 
							path.toFile().isDirectory() ?
                            filesInDir(path) :
                                Stream.of(path))
                //.map(path -> path.toFile().toPath())
                                ;
    }

    /**
     * Handled by google guava Throwables.propagate(e)
     */
    
    private static Stream<Path> listFiles(Path dir){
        try {
             return Files.list(dir);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }


   /**
     * Handled by custom exception unchecker
     */

    public static Stream<Path> filesInDirCustom(Path dir)  throws IOException  {
        return listFilesCustom(dir)
                .flatMap(path -> 
							path.toFile().isDirectory() ?
                            uncheckException( path -> FileUtil.listFilesCustom(path)) :
                                Stream.of(path))
                //.map(path -> path.toFile().toPath())
                                ;
    }

    /**
     * Handled by custom exception unchecker
     */
    
    private static Stream<Path> listFilesCustom(Path dir) throws IOException{
             return Files.list(dir);
    }


    /**
     * This customd stream exception unchecker is still under working , list here only for refrence.
     * idea come from  @see <a href="https://blog.codefx.org/java/repackaging-exceptions-streams/">dicussion</a> 
     */
    static <T, R> Function<T, R> uncheckException(CheckedFunction<T, R, Exception> function) 
        {
            return element -> {
                try {
                    return function.apply(element);
                } catch (Exception ex) {
                    if (ex instanceof RuntimeException)
                        throw (RuntimeException) ex;
                    else
                        throw new RuntimeException(ex);
                }
    };
}    

    public static void main(String[] args){

        final File home = new File("src");
        final Stream<Path> files = FileUtil.filesInDir(home.toPath());
        files.forEach(System.out::println);

    }

}