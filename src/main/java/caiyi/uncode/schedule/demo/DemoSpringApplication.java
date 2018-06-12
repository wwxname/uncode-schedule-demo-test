package caiyi.uncode.schedule.demo;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;

@SpringBootApplication
@EnableScheduling
//@ComponentScan(excludeFilters = {@ComponentScan.Filter(value = Component.class)})
public class DemoSpringApplication {

    public static  void main(String args[]){

        String myArgs[] = new String[3];
        myArgs[0]="-server";
        myArgs[1]="-Xms128m";
        myArgs[2]="-Xmx256m";
        SpringApplication.run(DemoSpringApplication.class,myArgs);
    }
}
