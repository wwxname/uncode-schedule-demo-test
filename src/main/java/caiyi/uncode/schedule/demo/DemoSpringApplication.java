package caiyi.uncode.schedule.demo;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DemoSpringApplication {

    public static  void main(String args[]){

        String myArgs[] = new String[3];
        myArgs[0]="-server";
        myArgs[1]="-Xms128m";
        myArgs[2]="-Xmx128m";
        SpringApplication.run(DemoSpringApplication.class,myArgs);
    }
}
