package io.stargate.auth.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;

public class AuthFileService implements AuthenticationService {
    // TODO: [doug] 2020-06-18, Thu, 0:48 make configurable
    private static final String FILENAME = "/tmp/tokens.txt";

    static {
        File f = new File(FILENAME);
        if (!f.exists()){
            try {
                f.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("Unable to create file " + FILENAME);
            }
        }
    }

    private final Function<String, StoredCredentials> mapToItem = (line) -> {
        String[] p = line.split(",");
        StoredCredentials item = new StoredCredentials();
        item.setRoleName(p[1]);
        item.setPassword(p[2]);

        return item;
    };

    @Override
    public String createToken(String key, String secret) {
        String token = UUID.randomUUID().toString();
        writeToFile(token + "," + key + "," + secret);
        return token;
    }

    private void writeToFile(String record) {
        try (FileWriter fileWriter = new FileWriter(FILENAME, true)) {
            fileWriter.write(record + "\n");

            fileWriter.flush();
        } catch (IOException e) {
            // TODO: [doug] 2020-06-18, Thu, 0:40 better log and handle error here
            throw new RuntimeException(e);
        }
    }

    @Override
    public StoredCredentials validateToken(String token) throws UnauthorizedException {
        List<StoredCredentials> inputList;
        try {
            if (token == null || token.equals("")) {
                throw new UnauthorizedException("Token must not be null");
            }

            File inputFile = new File(FILENAME);
            InputStream inputStream = new FileInputStream(inputFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

            inputList = br.lines()
                    .filter(line -> line.startsWith(token))
                    .map(mapToItem)
                    .collect(Collectors.toList());
            br.close();

            if (inputList.size() == 0) {
                throw new UnauthorizedException("Token not valid");
            }
        } catch (IOException e) {
            // TODO: [doug] 2020-06-18, Thu, 0:40 better log and handle error here
            throw new RuntimeException(e);
        }
        return inputList.get(0);
    }
}
