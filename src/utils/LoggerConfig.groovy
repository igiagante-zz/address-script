package utils
import org.apache.log4j.*
/**
 * Created by igiagante on 8/5/15.
 */
class LoggerConfig {

    private static String PATTERN = "%d{ABSOLUTE} %-5p [%c{1}] %m%n"

    public static configLogger(){
        def simple = new PatternLayout(PATTERN)
        BasicConfigurator.configure(new ConsoleAppender(simple))
        LogManager.rootLogger.level = Level.INFO
    }
}
