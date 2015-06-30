package services

import org.apache.log4j.Logger
import utils.Helper
import utils.LoggerConfig

/**
 * Created by igiagante on 7/5/15.
 * This class involves all the process related to addresses which do not have cityId, but they are available to be updated.
 * Once the cityId was included, the address procedures to be normalized.
 */
class CityControl {

    static final validFilePath = "/Users/igiagante/Documents/mexico/mexicoLive.txt"
    static final cityIdsFilePath = "/Users/igiagante/Documents/mexico/cityId/mexicoCityIds.txt"
    static final notValidFilePath = "/Users/igiagante/Documents/mexico/cityId/mexicoNotCityIds.txt"

    static final Logger log = Logger.getLogger(Helper.class)

    private static final Map<String, String> cityIdWithCityId = new HashMap<String, String>()

    public static void main(String[] args){

        final String VALID = "valid"
        final String NOT_VALID = "not valid"

        LoggerConfig.configLogger()

        log.info("Creating file of addresses available to be updated with cityIds")
        File cityIdsFile = Helper.createFile(cityIdsFilePath)

        log.info("Creating file with addresses which are not available to be updated")
        File notValidFile = Helper.createFile(notValidFilePath)

        log.info("Reading file with addresses without city_id")
        File validFile = new File(validFilePath)
        Map<String, String> addresses = Helper.readAddressFile(validFile)

        log.info("Collecting keys from valid addresses to be updated")
        def keysFromAddressesValid = addresses.findAll { isValid(it.key, it.value, "MX") == true }.collect{ it.key }

        log.info("Creating subMap from addresses valid. At this step, the addresses which passed the basic validation continue to next process.")
        def Map<String, String> addressesValidMap = addresses.subMap(keysFromAddressesValid)
        def List<String> addressesValidList = addressesValidMap.collect { it.key + ", " + it.value.trim() }

        log.info("Creating file with valid addresses")
        Helper.createFileAddresses(cityIdsFile, addressesValidList, VALID)

        log.info("Filtering not valid addresses")
        def List<String> addressesNotValidList = Helper.compareAdressess(addresses, addressesValidMap)

        log.info("Creating file with NOT valid addresses")
        Helper.createFileAddresses(notValidFile, addressesNotValidList, NOT_VALID)

        log.info("Creating map with addressId and UserId from cityIds file")
        final Map<String, String> addressIdAndUserIdMap = Helper.readAddressFile(cityIdsFile)

        log.info("Itearate the cityIds map and update each address using the correct cityId")
        addressIdAndUserIdMap.each { addressId, userId ->

            def json = "{ \"city\" : { \"id\": \"${cityIdWithCityId.get(addressId)}\" } }"

            def result = Helper.updateAddress(addressId, userId, json)

            if(result){
                log.info("the address with addressId ${addressId}, userId ${userId} was updated sucessfully")
            }else{
                log.info("AddressId ${addressId}, userId ${userId} wasnt updated sucessfully")
            }
        }
    }

    /**
     * It will be checked if an address is available to be updated
     * @param addressId
     * @param userId
     * @param country
     * @return Boolean
     */
    public static Boolean isValid(String addressId, String userId, String country){

        def valid = false

        try{
            log.info("Getting address info")
            def address = Helper.getAddress(addressId, userId)

            log.info("Creating a map with fields to determinate if the address is available to be updated with cityId")
            def validDataSet = [address_line : address.address_line, street_name : address.street_name, street_number : address.street_number, neighborhood : address.neighborhood.name]

            log.info("Getting city info")
            def zipCode = address.zip_code
            def cityIdFromAddress = address.city.id
            def city = Helper.getCity(country, zipCode)

            //check if city_id is null
            def cityId = city.city.id ?: null

            cityIdWithCityId.put(addressId, cityId)

            def list = validDataSet.findAll{ it.value != null }.collect{ it.key }

            if(list?.size() > 3 && cityIdFromAddress == null){
                valid = true
            }
            return valid;
        }
        catch(e)
        {
            log.error(e.message)
        }
    }
}
