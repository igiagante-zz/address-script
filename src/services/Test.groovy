package services

import org.apache.log4j.Logger
import utils.Helper
import utils.LoggerConfig

import java.text.Normalizer

/**
 * Created by igiagante on 7/5/15.
 * This class provide funcionality to test a specific scenario
 */
class Test {

    public static void main(String [] args){

        String decomposed = Normalizer.normalize("boGOtá", Normalizer.Form.NFD);
        // removing diacritics

        String removed = decomposed.replaceAll("\\p{InCombiningDiacriticalMarks}+", "")

        println removed.toLowerCase().capitalize()
    }
}
