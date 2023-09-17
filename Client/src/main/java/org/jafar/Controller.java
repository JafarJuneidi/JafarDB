package org.jafar;

import lombok.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/api/v1")
@CrossOrigin(origins = "*")
public class Controller {
    Driver driver = new Driver("https://localhost", 8090);

    public Controller() throws IOException {}


    public ResponseEntity<String> put(
            @RequestBody User user
    ) {
        try {
            driver.insert(user);
        } catch(IOException ioException) {
            System.out.println(ioException.getMessage());
        }

        return ResponseEntity.ok("Ok");
    }
}


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
class User {
    private String firstname;
    private String lastname;
    private String email;
}
