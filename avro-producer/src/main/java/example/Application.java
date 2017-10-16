package example;

import java.io.File;
import java.io.IOException;

import example.avro.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Created by David on 16/10/2017.
 */
public class Application {

    public static void main(String[] args) {
        User user = generatePayload();

        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        try {
            dataFileWriter.create(user.getSchema(), new File("users.avro"));
            dataFileWriter.append(user);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static User generatePayload() {
        return new User("John Doe", 7, "blue");
    }

}
