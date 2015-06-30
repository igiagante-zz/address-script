package utils

import exceptions.ExceptionCurlError
import groovy.time.TimeCategory
import groovy.time.TimeDuration

import java.text.SimpleDateFormat

/**
 * Created by igiagante on 4/6/15.
 */
class Elastic {

    public static void main(String [] args){

        final filePath = "/Users/igiagante/Documents/cities.txt"

        final countryCode = 'cl';

        File citiesFile = Helper.createFile(filePath)

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        Integer size = 500;
        String openTime = "5m";
        Integer countAddressDownload = 0

        def scrollObjectInit = Helper.obtainScrollId(countryCode, openTime, size.toString());

        String scrollId = scrollObjectInit._scroll_id

        def start = new Date();

        for (int k = 0; k < 1000; k++) {

            try{
                def result =  Helper.getAddressesFromColombia(scrollId, openTime)

                def list = result?.hits?.hits?.fields

                for (int i = 0; i < list?.size(); i++) {

                    def line =  list.get(i).get('id').toString().replace("[", "").replace("]", "")  + ", " +
                            list.get(i).get('user_id').toString().replace("[", "").replace("]", "")  + ", " +
                            list.get(i).get('city.name').get(0) + ", " +
                            list.get(i).get('state.id').get(0)

                    println(line)
                    citiesFile.append(line,"UTF-8")
                    citiesFile.append("\n")
                    countAddressDownload++
                }
                //adding new scrollId
                scrollId = result?._scroll_id
            }catch (Exception e){
                println(e.message)
            }

        }

        Date stop = new Date()
        TimeDuration timeDuration = TimeCategory.minus(stop, start)

        println("Number of address updated: ${countAddressDownload}")
        println("Time consumed: ${timeDuration.toString()}")
    }
}
