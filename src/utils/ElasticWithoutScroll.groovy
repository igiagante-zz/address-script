package utils

import java.text.SimpleDateFormat

/**
 * Created by igiagante on 5/6/15.
 */
class ElasticWithoutScroll {

    public static void main(String [] args){

        final filePath = "/Users/igiagante/Documents/citiesColombia.txt"

        File citiesFile = Helper.createFile(filePath)

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        Integer size = 50000;

        def start = System.currentTimeMillis();

        def result =  Helper.getAddressesFromColombia("", "0", size.toString())

        def list = result.hits.hits.fields

        for (int i = 0; i < list.size(); i++) {

            def line =  list.get(i).get('id').toString().replace("[", "").replace("]", "")  + ", " +
                    list.get(i).get('user_id').toString().replace("[", "").replace("]", "")  + ", " +
                    list.get(i).get('city.name').get(0) + ", " +
                    list.get(i).get('state.id').get(0)

            println(line)
            citiesFile.append(line,"UTF-8")
            citiesFile.append("\n")
        }

        def end = System.currentTimeMillis();

        Date time = new Date(end - start);
        println("Process Time: " +  sdf.format(time))

    }
}
