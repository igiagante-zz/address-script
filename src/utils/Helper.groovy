package utils

import exceptions.ExceptionCurlError
import groovy.json.JsonSlurper
import org.apache.log4j.*
import services.ColombiaFixIdNull

/**
 * Created by igiagante on 7/5/15.
 */
class Helper {

    static final Logger log = Logger.getLogger(Helper.class)

    public static File createFile(String path){

        def file = new File(path)

        if(file.exists()) {
            file.delete()
            file = new File(path)
        }

        return file
    }

    /**
     * It reads a file to extract addresses data and a map is created.
     * @param file
     * @return Map<String, String>
     */
    public static Map<String, String> readAddressFile(File file){

        def Map<String, String> addresses = new HashMap<String, String>();

        if(file.exists()){
            Scanner sc = new Scanner(file, "UTF-8")
            System.out.println( "Se carga address IDs & users IDs  " + file)

            while (sc.hasNextLine()) {
                def pair = sc.nextLine().split(",")
                addresses.put(pair[0], pair[1])
            }

            log.info("A map with address IDs y user IDs was created")
        }

        return addresses
    }

    /**
     * Create a file with addresses
     * @param validFile
     * @param addressesValid
     * @param type indicate the type of addresses which are going to be saved
     */
    public static void createFileAddresses(File validFile, List<String> addressesValid, String type){

        log.info( "Creating a file with addresses")

       // validFile.append("Number of addresses ${type}: " + addressesValid.size())
        //validFile.append("\n\n")

        addressesValid.each{
            validFile.append(it,"UTF-8")
            validFile.append("\n")
        }
    }

    /**
     * Compare maps of addresses and filter them which do not belong to correct set
     * @param Map<String, String> addresses
     * @param Map<String, String> addressesNotValid
     * @return List<String>
     */
    public static List<String> compareAdressess(Map<String, String> addresses, Map<String, String> addressesNotValid){

        List<String> addressesValid = new ArrayList<String>()

        addresses.each { key, value ->
            if(!addressesNotValid.containsKey(key)){
                addressesValid.add(key + ", " + value)
            }
        }

        return addressesValid
    }

    public static Boolean isNormalized(String addressId, String userId){
        def address = getAddress(addressId, userId)
        return address.normalized
    }

    /**
     * Wrapper for method GET from api addresses
     * @param addressId
     * @param userId
     * @return Object
     */
    public static Object getAddress(String addressId, String userId) {

         log.info("Getting data from addresses using addressId ${addressId} y userId ${userId}")

        def command = [
                'bash',
                '-c',
                "curl \"http://localhost:3500/addresses/${addressId.trim()}?caller.id=${userId.trim()}&caller.status=ACTIVE\" "
        ]

        return executeCommand(command)
    }

    /**
     * Wrapper for method UPDATE from api addresses
     * @param addressId
     * @param userId
     * @param json
     * @return Boolean
     */
    public static String updateAddress(String addressId, String userId, String json) throws ExceptionCurlError
    {

        log.info("addressId ${addressId} y userId ${userId} updated.")

        def command = [
                'bash',
                '-c',
                "curl -X PUT -H \'Content-Type: application/json\' -d \'${json}\' \'http://localhost:3500/addresses/${addressId.trim()}?caller.id=${userId.trim()}&caller.status=ACTIVE\'"
        ]

        return "curl -X PUT -H \'Content-Type: application/json\' -d \'${json}\' \'internal.mercadolibre.com/addresses/${addressId.trim()}?caller.id=${userId.trim()}&caller.status=ACTIVE\'"

       // return executeCommand(command)
    }

    /**
     * Wrapper for method GET from api addresses
     * @param country
     * @param zipCode
     * @return Object
     */
    public static Object getCity(String country, String zipCode){

        log.info("Getting data from cities using zipCode ${zipCode}")

        def command = [
                'bash',
                '-c',
                "curl \"http://localhost:3500/countries/${country}/zip_codes/${zipCode}\" "
        ]

        return executeCommand(command)
    }

    /**
     * Wrapper for method GET from api locations
     * @param stateId
     * @param cityName
     * @return Object
     */
    public static Object getCityFromLocationApi(String cityName, String stateId) throws ExceptionCurlError, IllegalArgumentException{

        def command = [
                'bash',
                '-c',
                "curl \"http://localhost:3500/locations_mapping?stateId=${stateId.trim()}&cityName=${cityName.trim().replace(" ", "+")}\" "
        ]

       // println(command)

        return executeCommand(command)
    }

    public static Object obtainScrollId(String countryCode, String openTime, String size){

        //addresses-elasticsearch.ml.com/co/address/_search
        def initCommand = [
                'bash',
                '-c',
                "curl 'addresses-elasticsearch.ml.com/${countryCode}/address/_search?search_type=scan&scroll=${openTime}&size=${size}' -d " +
                        "'{\"fields\": [\"id\",\"user_id\",\"city.name\",\"state.id\"],\"query\": {\n" +
                        "    \"filtered\": {\"query\": {\"match_all\": {}},\"filter\": {\"bool\": {\"must\": " +
                        "[{\"missing\": {\"field\": \"address.city.id\"}},{\"exists\": {\"field\": \"address.city.name\"}}," +
                        "{\"exists\": {\"field\": \"address.address_line\"}}]}}}}}' "
        ]
       // println(initCommand)
        return executeCommand(initCommand)
    }

    /**
     * Wrapper for method GET from api locations
     * @param stateId
     * @param cityName
     * @return Object
     */
    public static Object getAddressesFromColombia(String scrollId, String openTime){

        //To request the first bunch of data, its important to take out the index, verb or type_verb and add "scroll" path after _search
        //addresses-elasticsearch.ml.com/_search/scroll?scroll=5m&scroll_id=
        def command = [
                'bash',
                '-c',
                "curl 'addresses-elasticsearch.ml.com/_search/scroll?scroll=${openTime}&scroll_id=${scrollId}' -d " +
                        "'{\"fields\": [\"id\",\"user_id\",\"city.name\",\"state.id\"],\"query\": {\n" +
                        "    \"filtered\": {\"query\": {\"match_all\": {}},\"filter\": {\"bool\": {\"must\": " +
                        "[{\"missing\": {\"field\": \"address.city.id\"}},{\"exists\": {\"field\": \"address.city.name\"}}," +
                        "{\"exists\": {\"field\": \"address.address_line\"}}]}}}}}' "
        ]

        //println('command: ' + command)
        return executeCommand(command)

    }




    /**
     * Exectue a command in the bash
     * @param command
     * @return Object
     */
    static private Object executeCommand(def command){

        try{
            def sout = new StringBuffer() //Standart Output
            def serr = new StringBuffer() //Standart Error
            def proc = command.execute()
            proc.consumeProcessOutput(sout, serr)
            proc.waitForOrKill(10000)

            def slurper = new JsonSlurper()
            def result = slurper.parseText(sout.toString())

            if(result?.error){
                //println('Error: ' + result.error + '\t Message: '+ result.message)
                throw new ExceptionCurlError(result?.cause[0]?.message?: 'Error: ' + result.error + '\t Message: '+ result.message)
            }else{
                return result
            }
        }catch (IllegalArgumentException e){
            println(e.message)
            throw new Exception("   ....... check network connections!!! ....... ", e);
        }
    }
}
