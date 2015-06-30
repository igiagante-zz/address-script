package services

import org.apache.log4j.Logger

import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory
import java.nio.charset.StandardCharsets

/**
 * Created by igiagante on 12/5/15.
 */
class ZipcodesServiceStream {

    static final def isProductive = false
    static String cookie = null

    static final Logger log = Logger.getLogger(ZipcodesServiceStream.class)

    static Map<Integer, String> zipCodeTypeMap = new HashMap<Integer, String>()

    private static initZipCodeTypeMap(){

        zipCodeTypeMap.put(1, "LC")
        zipCodeTypeMap.put(2, "LO")
        zipCodeTypeMap.put(3, "LO")
        zipCodeTypeMap.put(4, "CP")
        zipCodeTypeMap.put(5, "GU - UP")
    }

    private String getZipCodeType(Integer zipCodeTypeKey){
        return zipCodeTypeMap.get(zipCodeTypeKey)
    }

    public static Map<String, String> getCepData(String cep){

        String urlProceso = "http://www.buscacep.correios.com.br/servicos/dnec/consultaEnderecoAction.do?relaxation="+cep+"&TipoCep=ALL&semelhante=N&cfm=1&Metodo=listaLogradouro&TipoConsulta=relaxation&StartRow=1&EndRow=10"

        log.info("Getting the main source data from Correo")

        String source = getWebSiteData(urlProceso)

        if(source.contains("o foi encontrado") || source.contains("Busca Inv") || source.contains("SESSAO EXPIRADA")){
            log.info("The zipcode wasn't found")
        }else {

            initZipCodeTypeMap()

            log.info("Getting a specific data for a zipcode")
            String tipoCep = getTipoCep(source)
            String urlDetailCep = createUrlDetalleCep(tipoCep, cep)
            String sourceDetail = getWebSiteData(urlDetailCep)
            Map<String, String> cepDataMap = proccessSourceDetail(sourceDetail)

            cepDataMap.put("zipCodeType", zipCodeTypeMap.get(tipoCep.toInteger()))

            return cepDataMap
        }
    }

    /**
     * A small parse with workarounds which helps to create a map with the zipcode data. This data is rendered
     * using <table> tag, so it's necessary to recreate a dom document in order to obtain the data without errors.
     * @param sourceDetail
     * @return
     */
    def static Map<String, String> proccessSourceDetail(String sourceDetail){

        def charIndex = sourceDetail.indexOf('<table xmlns:fo="http://www.w3.org/1999/XSL/Format">')

        if(charIndex == -1){
            println( "Table not found ")
        }else{

            //Get tables values. It could have come more than one table.
            String tables = sourceDetail.substring(charIndex, sourceDetail.size() - 1)
            charIndex = tables.indexOf('<br>')
            tables = tables.substring(0, charIndex)

            //Workaround to set attribute class=table. So, I can difference between tables.
            String twoTable  = tables.replace("xmlns:fo=\"http://www.w3.org/1999/XSL/Format\"", "class=\"table1\"")
            twoTable  = twoTable.replaceFirst("class=\"table1\"", "class=\"table0\"")

            //Get the main table, where is the zipcode data
            int start = twoTable.indexOf('<table class="table0">')
            int end = twoTable.indexOf('class="table1">')
            String mainTable =  twoTable.minus(twoTable.substring(start, end))
            mainTable = "<table " + mainTable

            return createTableMap(processXml(mainTable, '//table'))
        }
    }

    /**
     * Creates a document element in order to be evaluate by xpath
     * @param xml
     * @param xpathQuery
     * @return
     */
    def static processXml( String xml, String xpathQuery ) {
        def xpath = XPathFactory.newInstance().newXPath()
        def builder     = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        def inputStream = new ByteArrayInputStream( xml.bytes )
        def records     = builder.parse(inputStream).documentElement

        xpath.evaluate( xpathQuery, records )
    }

    /**
     * Creates a map which has the value from the parsed table
     * @param data
     * @return
     */
    private static Map<String, String> createTableMap(String data){

        List<String> cepData = data.split("\n")

        Map<String, String> tableMap = new LinkedHashMap<String, String>()

        for (int i = 0; i < cepData.size(); i++) {
            if(cepData.get(i).trim().contains("Logradouro:")){
                tableMap.put("Logradouro", cepData.get(i + 1).trim())
            }
            if(cepData.get(i).trim().contains("Cliente:")){
                tableMap.put("Cliente", cepData.get(i + 1).trim())
            }
            if(cepData.get(i).trim().contains("CPC:")){
                tableMap.put("CPC", cepData.get(i + 1).trim())
            }
            if(cepData.get(i).trim().contains("Promoção:")){
                tableMap.put("Promoção", cepData.get(i + 1).trim())
            }
            if(cepData.get(i).trim().contains("Endereço:")){
                tableMap.put("Endereço", cepData.get(i + 1).trim())
            }
            if(cepData.get(i).trim().contains("Bairro:")){
                tableMap.put("Bairro", cepData.get(i + 1).trim())
            }
            if(cepData.get(i).trim().contains("Localidade / UF:") || cepData.get(i).trim().contains("Localidade:")){
                tableMap.put("Localidade", cepData.get(i + 1).trim())
            }
            if(cepData.get(i).trim().contains("CEP:")){
                tableMap.put("CEP", cepData.get(i + 1).trim())
            }
            if(cepData.get(i).trim().contains("Abrangência:")){
                tableMap.put("Abrangência", cepData.get(i + 1).trim())
            }
        }

        println tableMap

        return tableMap
    }

    /**
     * Get source form a web site
     * @param urlAddress
     * @return
     */
    public static String getWebSiteData(String urlAddress){

        URL url = new URL(urlAddress)
        URLConnection urlConnection = null

        if(isProductive){
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("172.16.0.89", 80));
            urlConnection = url.openConnection(proxy)
        }
        else
        {
            urlConnection = url.openConnection()
        }

        def source = urlConnection.with { con ->

            // If we got a cookie last time round, then add it to our request
            if( cookie ) con.setRequestProperty( 'Cookie', cookie )
            con.connect()

            // Try and get a cookie the site will set, we will pass this next time round
            cookie = con.getHeaderField( "Set-Cookie" )

            // Read the HTML and close the inputstream
            con.inputStream.withReader(StandardCharsets.ISO_8859_1.toString(), { it.text })
        }
        return source
    }

    public static String createUrlDetalleCep(String tipoCep, String cep){
        return "http://www.buscacep.correios.com.br/servicos/dnec/detalheCEPAction.do?Metodo=detalhe&Posicao=1&TipoCep=" + tipoCep + "&CEP=" + cep
    }

    /**
     * It searches through the source a specific code to get an attribute called tipoCep.
     * This attribute is necessary to do a request in order to get all the data from one zipcode.
     * @param source
     * @return
     */
    public static String getTipoCep(String source){

        int charIndex = source.indexOf("javascript:detalharCep")
        if(charIndex == -1){
            return null
        }
        String tipoCep = source.substring(charIndex, source.size() -1)
        tipoCep = tipoCep.substring(28, 29)

        if((1 .. 5).contains(tipoCep.toInteger())){
            return tipoCep
        }else{
            "El tipo de CEP no es valido: " + tipoCep
        }
    }

    private static String formatCep(String cep){
        return ("00000000" + cep).substring(cep.length())
    }
}
