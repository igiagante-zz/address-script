package services

import utils.Helper
import utils.LoggerConfig

/**
 * Created by igiagante on 6/5/15.
 * This class analized the first stack of address not normalized and verify if it is possible to normalize each of them.
 */

class CheckAddressesImposible {

    static final mexicoFilePath = "/Users/igiagante/Documents/mexico/mexico.txt"
    static final validFilePath = "/Users/igiagante/Documents/mexico/mexicoLive.txt"
    static final notValidFilePath = "/Users/igiagante/Documents/mexico/mexicoDead.txt"

    static def mexicoFile =  new File(mexicoFilePath)
    static def validFile =  Helper.createFile(validFilePath)
    static def notValidFile =  Helper.createFile(notValidFilePath)

    public static void main(String[] args){

        LoggerConfig.configLogger()
        createAddressesList()
    }

    public static void createAddressesList(){

        //get addresses data
        def Map<String, String> addresses = Helper.readAddressFile(mexicoFile)

        //collect keys from addresses not valid
        def keysFromAddressesNotValid = addresses.findAll { isValid(it.key, it.value) == false }.collect{ it.key }

        //create subMap from addresses not valid
        def Map<String, String> addressesNotValidMap = addresses.subMap(keysFromAddressesNotValid)
        def List<String> addressesNotValidList = addressesNotValidMap.collect { it.key + ", " + it.value.trim() }

        //create file with addresses not valid
        Helper.createFileAddresses(addressesNotValidList)

        //create file with valid addresses
        def List<String> list = Helper.compareAdressess(addresses, addressesNotValidMap)
        Helper.createFileAddresses(list)
    }

    public static Boolean isValid(String addressId, String userId){

        def valid = false

        System.out.println( "Trying to get the data from addressId ${addressId} y userId ${userId}")

        try{
            def response = Helper.getAddress(addressId, userId)
            def retValue = [address_line : response.address_line, street_name : response.street_name, street_number : response.street_number, neighborhood : response.neighborhood.name]

            def list = retValue.findAll{ it.value != null }.collect{ it.key }

            if(list.size() > 3){
                System.out.println( " ${addressId}, ${userId} is valid ")
                valid = true
            }
            return valid;
        }
        catch(e)
        {
            System.out.println("Fail: " + e.printStackTrace())
        }
    }
}
