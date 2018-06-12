package caiyi.uncode.schedule.demo.test;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author juny.ye
 */
@Component
public class DemoSimpleTask {
    private static int i = 0;
   // @Scheduled(initialDelay = 1000,fixedDelay = 3000)
    public void print() {
        System.out.println("===========start!=========");
        System.out.println("I:"+i);i++;
        System.out.println("=========== end !=========");
    }
   // @Scheduled(initialDelay = 1000,fixedDelay = 3000)
    public void print1() {
        System.out.println("===========start!=========");
        System.out.println("print<<1>>:"+i);i++;
        System.out.println("=========== end !=========");
    }
}
