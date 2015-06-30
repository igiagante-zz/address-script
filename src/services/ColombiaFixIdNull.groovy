package services

import exceptions.ExceptionCurlError
import groovy.time.TimeCategory
import groovy.time.TimeDuration
import org.apache.log4j.FileAppender
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.TTCCLayout
import utils.Helper

import java.text.Normalizer

/**
 * Created by igiagante on 5/6/15.
 */
class ColombiaFixIdNull {

    static final filePath = "/Users/igiagante/Documents/chile/citiesChile.txt"
    static final cityIdsPath = "/Users/igiagante/Documents/chile/cityIds.txt"
    static final updatedCitiesPath = "/Users/igiagante/Documents/chile/input.txt"
    static final lastLinePath = "/Users/igiagante/Documents/chile/lastLine.txt"
    static final notUpdatedCitiesPath = "/Users/igiagante/Documents/colombia/notUpdatedCities.txt"

    private static final Map<String, String> cityNameStateId = new HashMap<String, String>()

    private static final Map<String, String> cityNameCityId= new HashMap<String, String>()

    private static final Map<String, List<String>> addressIdUserIdCityName = new HashMap<String, String>()

    private static final Map<String, String> secondaryMap = new HashMap<String, String>()

    private static final Map<String, Map<String, String>> mainMap = new HashMap<String, Map<String, String>>()

    static final Logger log = Logger.getLogger(Helper.class)

    static final logPath = "/Users/igiagante/Documents/chile/logger.log"

    static final def countAddressUpdated = 1

    static final File fileUpdatedCities = new File(updatedCitiesPath)

    static final File fileNotUpdatedCities = new File(notUpdatedCitiesPath)

    static final File lastLine = new File(lastLinePath)

    public static void main(String [] args) {

        Date start = new Date()

        log.level = Level.INFO
        // add an appender to log to file
        log.addAppender(new FileAppender(new TTCCLayout(), logPath));

        log.info("Reading file with addressId and userId")
        File file = new File(filePath)
        readAddressFile(file)

        log.info("Filling maps")
        fillMapWithFile()

        /*
        fileUpdatedCities.append("\n")
        fileUpdatedCities.append(" < --------- Starting new Updating Process ----------- > ${sdf.format(start)}","UTF-8")
        fileUpdatedCities.append("\n\n")

        fileNotUpdatedCities.append("\n")
        fileNotUpdatedCities.append("< --------- Starting new Updating Process ----------- ${sdf.format(start)}>","UTF-8")
        fileNotUpdatedCities.append("\n\n") */

        addressIdUserIdCityName.each{ addressId, list ->

            def userId = list.get(0)
            def cityName = list.get(1)
            def stateId = list.get(2)
            def cityId = getCityId(stateId, sanitizeCityName(cityName))

            if(cityId){
                def json = "{ \"city\" : { \"id\": \"${cityId}\" } }"

                try{
                    String line = Helper.updateAddress(addressId, userId, json)

                   // createFileCurls(line)

                    addAddressUpdated(addressId, userId, cityId)
                    println("${addressId},${userId} was updated sucessfully. Number: ${countAddressUpdated}")

                }catch (ExceptionCurlError e) {
                    log.info("AddressId ${addressId}, userId ${userId} wasn\'t updated sucessfully")
                   // addNotUpdatedAddress(addressId, userId, e.message)
                }
            }else{
                //if cityId is null, the response was a 404
                //addNotUpdatedAddress(addressId, userId, "cityId wasn\'t found for ${cityName}")
            }
        }

        Date stop = new Date()
        TimeDuration timeDuration = TimeCategory.minus(stop, start)

        log.info("Number of address updated: ${countAddressUpdated}")
        log.info("Time consumed: ${timeDuration.toString()}")

        /*
        fileUpdatedCities.append("\n\n")
        fileUpdatedCities.append("< --------- Finishing Updating Process ----------- > ${sdf.format(stop)}","UTF-8")
        fileUpdatedCities.append("\n")
        fileUpdatedCities.append("< --------- Time Consumed: ${timeDuration.toString()} ----------- >","UTF-8")
        fileUpdatedCities.append("\n\n")

        fileNotUpdatedCities.append("\n\n")
        fileNotUpdatedCities.append("< --------- Finishing Updating Process ----------- > ${sdf.format(stop)}","UTF-8")
        fileNotUpdatedCities.append("\n")
        fileNotUpdatedCities.append("< --------- Time Consumed: ${timeDuration.toString()} ----------- >","UTF-8")
        fileNotUpdatedCities.append("\n\n") */
    }


    public static void createFileCurls(String line){

        File file = new File("/Users/igiagante/Documents/colombia/curls.sh")

        file.append(line,"UTF-8")
        file.append("\n")

    }

    /**
     * It reads a file to extract addresses data and a map is created.
     * @param file
     * @return Map<String, String>
     */
    public static void readAddressFile(File file){

        if(file.exists()){
            Scanner sc = new Scanner(file, "UTF-8")
            System.out.println( "Se carga address IDs & users IDs  " + file)

            while (sc.hasNextLine()) {
                def four = sc.nextLine().split(",")
                try{

                    List<String> ternary = new ArrayList<String>()
                    ternary.add(four[1]) //userId
                    ternary.add(sanitizeCityName(four[2])) //cityName
                    ternary.add(four[3].trim()) //stateId

                    //Pushing Ternary data
                    addressIdUserIdCityName.put(four[0], ternary)
                }catch (ArrayIndexOutOfBoundsException e){
                    println("Error: " + four)
                    println(e.message)
                }

            }
            log.info("A map with address IDs y user IDs was created")
        }
    }

    public static String getCityId(String stateId, String cityName){

        if(mainMap?.get(stateId)?.get(cityName) == null){
            addNotUpdatedAddress(stateId, cityName, "cityId wasn\'t found in the main MAP")
        }

        return mainMap?.get(stateId)?.get(cityName)
    }

    public static void fillMapWithFile(){

        File file = new File(cityIdsPath)
        if(file.exists()){
            Scanner sc = new Scanner(file, "UTF-8")

            System.out.println( "Creating map with cityIds and cityNames  " + file)

            while (sc.hasNextLine()) {
                def third = sc.nextLine().split(",")
                //Key cityName, Object cityId
                secondaryMap.put(sanitizeCityName(third[2]), third[1])

                //Key StateId, Object cityNameCityId
                mainMap.put(third[0].trim(), secondaryMap)
            }
            log.info("A map with address IDs y user IDs was created")
        }
    }

    public static void fillMapWithApi(){

        //iterate the cityNames stateIds mpad
        cityNameStateId.each{ cityName, stateId ->

            //lets see if the id is in the citiNameCityId map
            if(cityNameCityId.get(cityName)){
                return cityNameCityId.get(cityName)
            }
            else{

                try{
                    //If the data are not in the map, it needs to ask the api for the cityId
                    def cityResponse = Helper.getCityFromLocationApi(cityName, stateId)

                    if (cityResponse?.status != 404) {

                        String cityId = cityResponse?.cityCore?.id
                        cityNameCityId.put(cityName, cityId)

                        //back up data into the file
                        addCityIdToFile(cityId, cityName)
                    }
                }catch (ExceptionCurlError e){
                    log.error(e.message.replace("/city/", cityName))
                }catch (IllegalArgumentException e){
                    log.error(e.message)
                }
            }
        }
    }

    public static void loadFileCityNameCityId(){

        File file = new File(cityIdsPath)

        if(file.exists()){
            Scanner sc = new Scanner(file, "UTF-8")
            log.info( " add CityName with CityId to map in order to keep in memory the data " + file)

            while (sc.hasNextLine()) {
                List<String> pair = sc.nextLine().split(",")
                //In the file the data are upside down
                cityNameCityId.put(pair[1], pair[0]);
            }
        }
    }

    public static String readCityFromFile(String cityName){

        File file = new File(cityIdsPath)

        if(file.exists()){
            Scanner sc = new Scanner(file, "UTF-8")
            log.info( "Check if cityId exists in the file  " + file)

            while (sc.hasNextLine()) {
                List<String> pair = sc.nextLine().split(",")
                if(!pair.isEmpty() && pair[1] == cityName)return pair[0];
            }
        }

        return "";
    }

    public static void addCityIdToFile(String cityId, String cityName){

        File file = new File(cityIdsPath)

        String line = cityId + "," + cityName

        file.append(line,"UTF-8")
        file.append("\n")
    }

    public static void addNotUpdatedAddress(String addressId, String userId, String messageError){

        String line = addressId + "," + userId + ", " + messageError

        log.error(messageError)

        fileNotUpdatedCities.append(line,"UTF-8")
        fileNotUpdatedCities.append("\n")
    }

    public static void addAddressUpdated(String addressId, String userId, String cityId){

        String line = addressId.trim() + "," + userId.trim() + ',' + cityId.trim()

        fileUpdatedCities.append(line,"UTF-8")
        fileUpdatedCities.append("\n")

        lastLine.write(line)

        countAddressUpdated++
    }

    public static String sanitizeCityName(String cityName){
        String decomposed = Normalizer.normalize(cityName, Normalizer.Form.NFD);
        // removing diacritics
        String cleanedText = decomposed.replaceAll("\\p{InCombiningDiacriticalMarks}+", "")

        return cleanedText.toLowerCase().capitalize().trim()
    }
}
