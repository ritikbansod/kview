package com.ritikbansod.kafkawrapper;

import com.ritikbansod.kafkawrapper.cli.CliMain;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaWrapperApplication {

    public static void main(String[] args) {
        // direct-mode CLI: `java -jar kview.jar --cli <command> ...` talks to the
        // broker without starting the web server
        if (args.length > 0 && (args[0].equals("--cli") || args[0].equals("cli"))) {
            String[] cliArgs = new String[args.length - 1];
            System.arraycopy(args, 1, cliArgs, 0, cliArgs.length);
            System.exit(CliMain.run(cliArgs));
        }
        SpringApplication.run(KafkaWrapperApplication.class, args);
    }
}
