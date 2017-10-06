package demo.lin1000.utility;

import com.google.common.base.Throwables;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileUtil{

    public static Stream<Path> filesInDir(Path dir)  {
        return listFiles(dir)
                .flatMap(path -> 
							path.toFile().isDirectory() ?
                                filesInDir(path) :
                                Stream.of(path))
                //.map(path -> path.toFile().toPath())
                                ;
    }
    
    private static Stream<Path> listFiles(Path dir) {
        try {
            return Files.list(dir);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void main(String[] args){

        final File home = new File("src");
        final Stream<Path> files = FileUtil.filesInDir(home.toPath());
        files.forEach(System.out::println);

    }

}