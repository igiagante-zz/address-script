import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.sql.Sql

import java.nio.charset.Charset
import java.text.Normalizer
import java.text.Normalizer.Form

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future


class UpdateZipCodeBr {


	// Datos de conexion para la base de shipping.
	static final def driverClassName = "oracle.jdbc.driver.OracleDriver"
	static  def username = "BULK_DB"
	static  def password = "oyyvIU60"
	static final def dbUrl = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=cluster04-scan.melirac.com)(PORT=1521))(CONNECT_DATA=(SERVER = DEDICATED)(SERVICE_NAME=shp_w01_access)))"

	static int cantUpd = 0
	static int cantDel = 0
	static int cantFail = 0
	static int countRows = 0
	static int reActivated = 0

	static List existingCeps = null

	static def meliStates = null
	static String updateQuery = 'UPDATE SHP_W01.ZIPCODE_MIGRATION SET  CITY_ID = \'[CITY_ID]\', STATE_ID  = \'[STATE_ID]\',CITY_NAME  = \'[CITY_NAME]\',CITY_TYPE  = \'[CITY_TYPE]\',ADDRESS  = \'[ADDRESS]\',STATE_NAME  = \'[STATE_NAME]\',ZIP_CODE_TYPE_ID  = \'[ZIP_CODE_TYPE_ID]\',NEIGHBORHOOD  = \'[NEIGHBORHOOD]\',OWNER_NAME= [OWNER_NAME],STATUS = \'active\' WHERE ZIP_CODE = \'[ZIP_CODE]\' AND COUNTRY_ID = \'BR\';'
	static String insertQuery = 'INSERT INTO SHP_W01.ZIPCODE_MIGRATION (ZIP_CODE, CITY_ID, STATE_ID, CITY_NAME, CITY_TYPE, ADDRESS, STATE_NAME, ZIP_CODE_TYPE_ID, NEIGHBORHOOD, OWNER_NAME, STATUS, COUNTRY_ID) VALUES (\'[ZIP_CODE]\', \'[CITY_ID]\', \'[STATE_ID]\',\'[CITY_NAME]\',\'[CITY_TYPE]\',\'[ADDRESS]\',\'[STATE_NAME]\',\'[ZIP_CODE_TYPE_ID]\',\'[NEIGHBORHOOD]\', [OWNER_NAME],  \'active\', \'BR\' );'
	static String inactiveQuery = 'UPDATE SHP_W01.ZIPCODE_MIGRATION SET STATUS = \'inactive\' WHERE ZIP_CODE = \'[ZIP_CODE]\' AND COUNTRY_ID = \'BR\';'
	static def countryInfo = null
	static def cityCepInfo = null
	static def sqlShipping = null

	static def fileApiPath =  new File("/Users/igiagante/Downloads/upd/api/mlbApi.txt")
	static def fileCepsPath =  new File("/Users/igiagante/Downloads/upd/ceps.txt")
	static def fileCapitaisPath =  new File("/Users/igiagante/Downloads/upd/csv/cep_capitais.csv")
	static def fileQueryPath =  new File("/Users/igiagante/Downloads/upd/query.txt")
	static def fileInactivePath =  new File("/Users/igiagante/Downloads/upd/inactive.txt")

	static final def isProductive = false
	static final def updateRecords = false
	static final Boolean loadCepFile = true
	static final int threadCount = 1

	static final Boolean compareAddress = false
	static final Boolean compareNeighborhood = false
	static final Boolean compareCity = true
	static final Boolean compareState = true
	static final Boolean compareOwner = false

	// Punto de entrada del script
	// Args contiene los parametros para usar el script.
	// [0] Debe indicar si la base es un delta "d" o si es la completa "c"
	public static void main(String[] args) {

		if(isProductive){

			username = "shp_w01prod"
			password = "k57DvNTA"

			fileApiPath =  new File("/data/slp/gscripts/upd/api/mlbApi.txt")
			fileCepsPath =  new File("/data/slp/gscripts/upd/ceps.txt")
			fileCapitaisPath =  new File("/data/slp/gscripts/upd/csv/cep_capitais.csv")
			fileQueryPath =  new File("/data/slp/gscripts/upd/query.txt")
			fileInactivePath =  new File("/data/slp/gscripts/upd/inactive.txt")
		}

		if(fileQueryPath.exists() ) {
			fileQueryPath.delete()
		}

		System.out.println( "Comienzo de ejecucion del script" );

		def slurper = new JsonSlurper()

		if(fileQueryPath.exists() ) {
			fileQueryPath.delete()
		}

		String apiResult

		if(!fileApiPath.exists()){

			// Cargo la informacion de los estados y ciudades desde la API de Countries.
			apiResult = GetApiInfo(false, null)

			if(!apiResult){
				System.out.println( "No se obtuvo el mapeo de la API de BR." )
				System.out.println( "Proceso finalizado con Error!." )
				return
			}

			countryInfo = slurper.parseText(apiResult)
			def cityInfo

			countryInfo.states.each{
				apiResult = GetApiInfo(true,it.id)
				cityInfo = slurper.parseText(apiResult)

				cityInfo.cities.each{
					it.putAt("normalized", NormalizeText(it.name))
				}

				it.putAt("normalized", NormalizeText(it.name))
				it.put("items", cityInfo)
			}

			cityInfo = null

			System.out.println( "Mapa cargado correctamente.")

			String json = JsonOutput.toJson(countryInfo)

			fileApiPath.withWriter('UTF-8') { writer ->
				writer.write(json)
			}

			System.out.println( "Archivo de mapa creado: " + fileApiPath)
			json = null
		}
		else{
			apiResult =  fileApiPath.getText('UTF-8')
			countryInfo = slurper.parseText(apiResult)
		}

		if( !fileCapitaisPath.exists() ) {
			System.out.println( "No se encontro el archivo de sociacion de ceps en la siguiente ruta: " + fileCapitaisPath)
			System.out.println( "Proceso finalizado con Error!." )
			return
		}


		System.out.println( "Ruta Asociacion Ceps: " + fileCapitaisPath)
		System.out.println( "Ruta Api MLB: " + fileApiPath)

		System.out.println( "Cargando archivo de asociacion de Ceps.")
		slurper = new JsonSlurper()
		String mapInfo = GetCityAssociation(fileCapitaisPath)

		if(!mapInfo){
			System.out.println( "No se obtuvieron datos del archivo de asociacion de Ceps en: " +fileCapitaisPath)
			System.out.println( "Proceso finalizado con Error!." )
			return
		}

		cityCepInfo = slurper.parseText(mapInfo)
		mapInfo = null
		System.out.println( "Archivo de asociacion cargado.")

		System.out.println( "Comenzando carga de Ceps activos, puede demorar....")

		if(!LoadActiveCeps()){
			System.out.println( "No se pudieron cargar los Ceps activos.")
			System.out.println( "Proceso finalizado con Error!." )
			return
		}

		System.out.println( "Se cargaron "+existingCeps.size()+" Ceps.")

		meliStates = slurper.parseText('[{"id":"AC","name":"Acre"},{"id":"AL","name":"Alagoas"},{"id":"AP","name":"Amapá"},{"id":"AM","name":"Amazonas"},{"id":"BA","name":"Bahia"},{"id":"CE","name":"Ceará"},{"id":"DF","name":"Distrito Federal"},{"id":"ES","name":"Espírito Santo"},{"id":"GO","name":"Goiás"},{"id":"MA","name":"Maranhão"},{"id":"MT","name":"Mato Grosso"},{"id":"MS","name":"Mato Grosso do Sul"},{"id":"MG","name":"Minas Gerais"},{"id":"PR","name":"Paraná"},{"id":"PB","name":"Paraíba"},{"id":"PA","name":"Pará"},{"id":"PE","name":"Pernambuco"},{"id":"PI","name":"Piauí"},{"id":"RN","name":"Rio Grande do Norte"},{"id":"RS","name":"Rio Grande do Sul"},{"id":"RJ","name":"Rio de Janeiro"},{"id":"RO","name":"Rondônia"},{"id":"RR","name":"Roraima"},{"id":"SC","name":"Santa Catarina"},{"id":"SE","name":"Sergipe"},{"id":"SP","name":"São Paulo"},{"id":"TO","name":"Tocantins"}]')

		System.out.println( "Comienzo del proceso." );
		System.out.println( "Conectando a la base de datos." );
		sqlShipping = Sql.newInstance(dbUrl,username, password, driverClassName)
		System.out.println( "Se pudo conectar a la base de datos." );

		int fileCount = existingCeps.size() -1

		def threadPool = Executors.newFixedThreadPool(threadCount) // Poner aca poolSize
		List<Future> futures
		try {
			futures = (0..fileCount).collect{index->
				threadPool.submit({-> Start(index) } as Callable);
			}
			futures.each{it.get()}
		}finally {
			threadPool.shutdown()
		}
		threadPool = null

		fileApiPath =  null
		fileCepsPath =  null
		fileCapitaisPath =  null
		fileQueryPath =  null

		System.out.println( "Cantidad total de registros: " + countRows)
		System.out.println( "Cantidad de Ceps para actualizar: " + cantUpd)
		System.out.println( "Cantidad de Ceps para eliminar: " + cantDel)
		System.out.println( "Cantidad de Ceps para setear manualmente: " + cantFail)

		System.out.println( "Fin de ejecucion del script." )
	}

	// Obtiene la informacion de al API para devolver los estados o ciudades.
	public static String GetApiInfo(getStateInfo, stateId){

		URL apiUrl = new java.net.URL("https://api.mercadolibre.com/countries/BR")
		if(isProductive){
			apiUrl = new java.net.URL("http://internal.mercadolibre.com/countries/BR")
		}

		if(getStateInfo == true){
			apiUrl = new java.net.URL("https://api.mercadolibre.com/states/" + stateId)
			if(isProductive){
				apiUrl = new java.net.URL("http://internal.mercadolibre.com/states/" + stateId)
			}
		}

		System.out.println( "Cargando Mapa desde API de countries desde " + apiUrl)

		URLConnection connection = apiUrl.openConnection()
		connection.setRequestMethod("GET")
		connection.setRequestProperty("Content-Type", "application/json")
		connection.setRequestProperty("Accept", "application/json")
		connection.connect()

		InputStream is = null

		is = connection.getInputStream()

		BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"))

		StringBuilder sb = new StringBuilder()
		String line
		while ((line = reader.readLine()) != null) {
			sb.append(line)
		}

		line = null
		is.close()
		connection.disconnect()

		apiUrl = null
		is = null
		reader = null

		return sb.toString()

	}

	// Lee el archivo de asociaciones de CEP para luego saber que tipo de ciudad es.
	public static String GetCityAssociation(filePath){

		Scanner sc = new Scanner(filePath, "UTF-8")
		String line = ""
		String[] fields
		while (sc.hasNextLine()) {
			fields = sc.nextLine().toString().split(';')
			line += '{"name":"'+fields[0]+'","stateId":"'+fields[1]+'","city":"'+fields[2]+'","start":"'+fields[3].replace("-","")+'","end":"'+fields[4].replace("-","")+'"},'
		}
		line = '{"items":[' + line.substring(0, line.length() - 1) + ']}'
		sc = null
		fields = null
		return line
	}

	// Obtiene el tipo de ciudad.
	public static String GetCityType(stateId, cepCode){

		String retValue = "CI"

		if(!cityCepInfo){
			return retValue
		}

		def cepInfo = cityCepInfo.items.findAll{it.stateId == stateId}

		if(cepInfo){
			def range
			cepInfo.each{
				range = Integer.parseInt(it.start)..Integer.parseInt(it.end)
				if(range.contains(Integer.parseInt(cepCode))){
					retValue = "CP"
					return
				}
			}
			range = null
		}
		cepInfo = null
		return retValue
	}

	// Obtiene la informacion del estado y ciudad desde la API.
	public static Map GetLocationName(String cityName, String stateId){

		def retValue = null

		//cityName = Charset.forName("UTF-8").encode(cityName)
		String cityNameNormalized = NormalizeText(cityName)

		def stateInfo = countryInfo.states.findAll{it.id == stateId}
		boolean cityFound = false

		if(stateInfo){
			stateInfo.items[0].cities.each{
				if(it.normalized == cityNameNormalized){
					retValue = [stateId : stateInfo.id, stateName : stateInfo.name, cityId : it.id, cityName : it.name]
					cityFound = true
					return
				}
			}
		}

		if(!cityFound){
			
			
			// No se encontro la ciudad hay que meterla
			// curl -X POST -H 'Content-type:application/json' -d '{"name":"Táchira","state":{"id":"VE-S"}}' 'internal.mercadolibre.com/internal/cities'

			// En caso de error porque existe la ciudad devuelve:
			// {"message":"The city already exists.","error":"bad_request","status":400,"cause":[]}

			// Si no existe la crea y devuelve el id generado:
			// "id":"QVItWEFsdGEgR3JhY2lh","name":"Alta Gracia","state":{"id":"AR-X","name":"Córdoba"},"country":{"id":"AR","name":"Argentina"},"geo_information":null}

			if(fileApiPath.exists()){
				fileApiPath.delete()
			}

			String json='{"name":"'+cityName+'","state":{"id":"'+stateId+'"}}'

			System.out.println( "Nueva Ciudad: curl -X POST -H 'Content-type:application/json' -d '{\"name\":\""+cityName+"\",\"state\":{\"id\":\""+stateId+"\"}}' 'internal.mercadolibre.com/internal/cities')")
			writeQuery("Nueva Ciudad: curl -X POST -H 'Content-type:application/json' -d '{\"name\":\""+cityName+"\",\"state\":{\"id\":\""+stateId+"\"}}' 'internal.mercadolibre.com/internal/cities')")

			return null
			
			
			try{

				def command = [
					'bash',
					'-c',
					"curl -X POST -H \"Content-Type: application/json\" -d '${json}' internal.mercadolibre.com/internal/cities"
				]

				def proc = command.execute()
				proc.waitFor()

				json = null

				if(proc.in.text.contains("error")){
					retValue = null
				}
				else{
					def slurper = new JsonSlurper()
					def response = slurper.parseText(proc.in.text)
					retValue = [stateId : stateInfo.id, stateName : stateInfo.name, cityId : response.id, cityName : response.name]

					countryInfo.states.each{
						if(it.id ==  stateInfo.id){
							it.items.cities.add('{id='+response.id+',name='+cityName+',normalized='+cityNameNormalized+'}')
							return
						}
					}
					slurper = null
					response = null
				}
			}
			catch(e)
			{
				System.out.println("Error al agregar la ciudad: " + e.printStackTrace())
				retValue = null
			}
		}

		cityName = null
		cityNameNormalized = null
		stateInfo = null

		return retValue
	}


	public static String NormalizeText(textInput){
		return Normalizer.normalize(textInput.toLowerCase(), Form.NFD).replaceAll(/\p{InCombiningDiacriticalMarks}+/, '').replaceAll(",", "").replaceAll("-", "").replaceAll(" ","").replaceAll(";","")
	}


	public static boolean LoadActiveCeps(){
		boolean retValue = false

		existingCeps = new LinkedList<String>();

		def activeCeps = null

		if(loadCepFile && fileCepsPath.exists()){
			// [{ZIP_CODE=45987059}, {ZIP_CODE=24733160}, {ZIP_CODE=60811080}, {ZIP_CODE=29138373}]
			Scanner sc = new Scanner(fileCepsPath, "UTF-8")
			System.out.println( "Se esta por cargar el archivo de Ceps en  "+fileCepsPath)

			while (sc.hasNextLine()) {
				existingCeps.add(sc.nextLine())
			}
			sc = null

			System.out.println( "Se cargo el archivo y se omitio la llamada a la base de datos.")
			retValue = true

		}
		else{
			sqlShipping = Sql.newInstance(dbUrl,username, password, driverClassName)
			activeCeps = sqlShipping.rows('SELECT ZIP_CODE FROM SHP_W01.ZIPCODE_MIGRATION WHERE COUNTRY_ID = \'BR\'')
			sqlShipping.close()
			sqlShipping = null
		}

		if(activeCeps && activeCeps.size() >0){
			retValue = true

			if(!fileCepsPath.exists() && loadCepFile){

				System.out.println( "Se esta generando el archivo de Ceps en  "+fileCepsPath)
				for(rw in activeCeps){
					fileCepsPath << (rw["ZIP_CODE"] + System.getProperty("line.separator"))
					existingCeps.add(rw["ZIP_CODE"])
				}
				System.out.println( "Se termino la generacion del archivo de Ceps.")
			}
		}

		activeCeps = null

		return retValue
	}

	static void writeQuery(def content) {

		def logContent = content + System.getProperty("line.separator")
		fileQueryPath.append(logContent,"UTF-8")
		logContent = null
	}

	static void writeInactive(def content) {
		def logContent = content + System.getProperty("line.separator")
		fileInactivePath.append(logContent,"UTF-8")
		logContent = null
	}

	public static def Start(int cepIndex){


		def locCity
		def cepRow = null
		def correiosApiInfo
		String correiosApiResult = null

		boolean updateData = false
		String stateId = null
		String stateName = null
		String cityName = null

		String newStateId = null
		String newStateName = null
		String newCityId = null
		String newCityName = null
		String newCityType = null
		String newAddress = null
		String newNeighborhood = null
		String newOwner = null
		String zipCodeTypeId = null

		String dynamicQuery = null

		String normalizedApiValue = null
		String normalizedRowValue = null

		def slurper = new JsonSlurper()

		String zeroFixCep = null
		String item = existingCeps[cepIndex]
		def insertCep = true;

		try{


			countRows++

			// En correios el largo de los ceps es de 8 caracteres si tiene menos hay que agregarle 0 adelante.
			item = ("00000000" + item).substring(item.length())
			zeroFixCep = item

			// Pero en nuestra base los 0 adelante no estan asi que debo sacarlos para las querys.
			if(item.substring(0,1) == "0"){
				zeroFixCep = item.trim().replaceFirst("^0*", "")
			}

			System.out.println("row: " +  countRows +  " cep: " + item + " - zeroFixCep: " + zeroFixCep )
			updateData = false

			try
			{
				cepRow = sqlShipping.rows("SELECT * FROM SHP_W01.ZIPCODE_MIGRATION WHERE ZIP_CODE = '"+zeroFixCep+"' AND COUNTRY_ID = 'BR'")
			}
			catch(e)
			{
				sqlShipping = null
				sqlShipping = Sql.newInstance(dbUrl,username, password, driverClassName)
				cepRow = sqlShipping.rows("SELECT * FROM SHP_W01.ZIPCODE_MIGRATION WHERE ZIP_CODE = '"+zeroFixCep+"' AND COUNTRY_ID = 'BR'")
			}


			if(cepRow && cepRow.size() >0){
				insertCep = false;
			}
			// {"cep":"37902121","logradouro":"Arlindo Figueiredo","bairro":"São Francisco","cidade":"Passos","estado":"MG"}

			// Busco los tipos de ceps
			correiosApiResult = GetCorreiosInfo(item)

			if(correiosApiResult != null){

				if(correiosApiResult == "NOT_FOUND"){
					dynamicQuery = inactiveQuery.replace("[ZIP_CODE]", zeroFixCep)

					System.out.println("QUERY: " + dynamicQuery)
					writeQuery(dynamicQuery)

					if(updateRecords){
						try{
							sqlShipping.execute(dynamicQuery.toString())
							sqlShipping.commit()
						}
						catch(e)
						{
							sqlShipping = Sql.newInstance(dbUrl,username, password, driverClassName)
							sqlShipping.execute(dynamicQuery.toString())
							sqlShipping.commit()
						}
					}

					cantUpd++
					return
				}

				correiosApiInfo = slurper.parseText(correiosApiResult)

				System.out.println(correiosApiInfo )


				if(correiosApiInfo){

					if(insertCep == false){

						if(cepRow["STATUS"] == "inactive"){
							reActivated++
							System.out.println(countRows + " - CEP: " +  item +  " estaba inactive y ahora correios lo reactivo." )
						}


						if(compareAddress){
							normalizedApiValue = NormalizeText(correiosApiInfo.logradouro.toString().toLowerCase())
							normalizedRowValue = NormalizeText(cepRow["ADDRESS"][0].toString().toLowerCase())

							if(normalizedApiValue != normalizedRowValue){
								updateData = true
							}
						}

						if(compareNeighborhood){
							normalizedApiValue = NormalizeText(correiosApiInfo.bairro.toString().toLowerCase())
							normalizedRowValue = NormalizeText(cepRow["NEIGHBORHOOD"][0].toString().toLowerCase())

							if(normalizedApiValue != normalizedRowValue){
								updateData = true
							}
						}
					}

					cityName = correiosApiInfo.cidade.toString()

					if(compareCity && insertCep == false){

						normalizedApiValue = NormalizeText(cityName.toLowerCase())
						normalizedRowValue = NormalizeText(cepRow["CITY_NAME"][0].toString().toLowerCase())

						if(normalizedApiValue != normalizedRowValue){
							updateData = true
						}
					}

					if(compareOwner && insertCep == false){
						if(correiosApiInfo.cliente.toString() != "null"){
							normalizedApiValue = NormalizeText(correiosApiInfo.cliente.toString().toLowerCase())
							normalizedRowValue = NormalizeText(cepRow["OWNER"][0].toString().toLowerCase())

							if(normalizedApiValue != normalizedRowValue){
								updateData = true
							}
						}
					}

					stateId = "BR-" + correiosApiInfo.estado.toString().toUpperCase()
					stateName = null

					meliStates.each(){
						if(it.id == correiosApiInfo.estado.toString().toUpperCase()){
							stateName = it.name
						}
					}

					if(!stateName){
						cantFail++
						System.out.println(countRows + " - CEP: " +  item +  " no se encontro el nombre del estado " + stateId )

						locCity = null
						cepRow = null
						correiosApiInfo
						correiosApiResult = null
						stateId = null
						stateName = null
						cityName = null
						newStateId = null
						newStateName = null
						newCityId = null
						newCityName = null
						newCityType = null
						newAddress = null
						newNeighborhood = null
						newOwner = null
						zipCodeTypeId = null
						dynamicQuery = null
						normalizedApiValue = null
						normalizedRowValue = null
						slurper = null
						zeroFixCep = null

						return
					}

					if(compareState && insertCep == false){
						if(stateId != cepRow["STATE_ID"][0].toString().toUpperCase()){
							updateData = true
						}
					}

					newStateId = stateId
					newStateName = stateName
					newCityName = cityName.replace("'", "''")
					newCityType = GetCityType(correiosApiInfo.estado.toString().toUpperCase(), item)
					newAddress = correiosApiInfo.logradouro.toString().replace("'", "''")
					newNeighborhood = correiosApiInfo.bairro.toString().replace("'", "''")
					newOwner = correiosApiInfo.cliente.toString().replace("'", "''")
					zipCodeTypeId =  correiosApiInfo.type.toString()
					newCityId = null

					locCity = GetLocationName(cityName, stateId)
					if(locCity){
						newCityId = locCity.cityId
					}

					if(!newCityId){
						cantFail++
						System.out.println("CEP: " +  item +  " no se encontro el city ID para la ciudad  " + cityName + " del estado " + stateId)

						locCity = null
						cepRow = null
						correiosApiInfo
						correiosApiResult = null
						stateId = null
						stateName = null
						cityName = null
						newStateId = null
						newStateName = null
						newCityId = null
						newCityName = null
						newCityType = null
						newAddress = null
						newNeighborhood = null
						newOwner = null
						zipCodeTypeId = null
						dynamicQuery = null
						normalizedApiValue = null
						normalizedRowValue = null
						slurper = null
						zeroFixCep = null

						return

					}

					dynamicQuery = ""

					if(updateData && insertCep == false){

						System.out.println( countRows + " - Diferencia encontrada en el CEP: " + item )


						// ZIP_CODE_TYPE
						// 770 - Grande Usuario GU
						// 771 - Caja Postal CP
						// 772 - Logradouro LO
						// 773 - Localidad LC
						// 4013178 - Unidad Operacional UO

						dynamicQuery = updateQuery.replace("[ZIP_CODE]", zeroFixCep).replace("[CITY_ID]", newCityId).replace("[STATE_ID]", newStateId).replace("[CITY_NAME]", newCityName)
								.replace("[CITY_TYPE]", newCityType).replace("[ADDRESS]", newAddress).replace("[STATE_NAME]",newStateName).replace("[ZIP_CODE_TYPE_ID]", zipCodeTypeId)
								.replace("[NEIGHBORHOOD]", newNeighborhood).replace("[OWNER_NAME]", newOwner)

						cantUpd++
					}

					if(insertCep == true)
					{
						System.out.println( countRows + " - Nuevo CEP encontrado: " + item )

						dynamicQuery = insertQuery.replace("[ZIP_CODE]", zeroFixCep).replace("[CITY_ID]", newCityId).replace("[STATE_ID]", newStateId).replace("[CITY_NAME]", newCityName)
								.replace("[CITY_TYPE]", newCityType).replace("[ADDRESS]", newAddress).replace("[STATE_NAME]",newStateName).replace("[ZIP_CODE_TYPE_ID]", zipCodeTypeId)
								.replace("[NEIGHBORHOOD]", newNeighborhood).replace("[OWNER_NAME]", newOwner)

						cantUpd++

					}

					if(dynamicQuery != ""){
						System.out.println("QUERY: " + dynamicQuery)
						writeQuery(dynamicQuery)
					}

					if(updateRecords){
						try{
							sqlShipping.execute(dynamicQuery.toString())
							sqlShipping.commit()
						}
						catch(e)
						{
							sqlShipping = Sql.newInstance(dbUrl,username, password, driverClassName)
							sqlShipping.execute(dynamicQuery.toString())
							sqlShipping.commit()
						}
					}
				}

			}else{
				System.out.println("CEP: " +  item +  " la busqueda en correios no dio resultados...")
				writeInactive(item)
			}

			locCity = null
			cepRow = null
			correiosApiInfo
			correiosApiResult = null
			stateId = null
			stateName = null
			cityName = null
			newStateId = null
			newStateName = null
			newCityId = null
			newCityName = null
			newCityType = null
			newAddress = null
			newNeighborhood = null
			zipCodeTypeId = null
			dynamicQuery = null
			normalizedApiValue = null
			normalizedRowValue = null
			slurper = null
			zeroFixCep = null

		}
		catch(e)
		{
			System.out.println("Start - Ocurrio un error: " + e.printStackTrace())
		}

	}

	public static String GetCorreiosInfo(String Cep){

		String source = null
		String urlProceso = "http://www.buscacep.correios.com.br/servicos/dnec/consultaEnderecoAction.do?relaxation="+Cep+"&TipoCep=ALL&semelhante=N&cfm=1&Metodo=listaLogradouro&TipoConsulta=relaxation&StartRow=1&EndRow=10"

		URL webUrl = null
		URLConnection connection = null
		Proxy proxy = null

		InputStream is = null
		StringBuilder sb = new StringBuilder()

		String headerName = null;
		String myCookie = null;

		while(!source)
		{
			webUrl = new java.net.URL(urlProceso)
			CookieHandler.setDefault( new CookieManager( null, CookiePolicy.ACCEPT_ALL ) );

			connection = null
			proxy = null

			if(isProductive){
				proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("172.16.0.89", 80))
				connection = webUrl.openConnection(proxy)
			}else
			{
				connection = webUrl.openConnection()
			}

			try{
				connection.setRequestMethod("GET")
				connection.setRequestProperty("Accept-Charset", "ISO-8859-1");
				connection.setRequestProperty("Content-Type", "application/json; charset=ISO-8859-1")
				connection.setRequestProperty("Accept", "application/json")
				connection.connect()

				for (int i=1; (headerName = connection.getHeaderFieldKey(i)) != null; i++) {
					if (headerName.toUpperCase().equals("SET-COOKIE")){
						myCookie = connection.getHeaderField(i)
						break
					}
				}

				is = connection.getInputStream()
				BufferedReader reader = new BufferedReader(new InputStreamReader(is, "ISO-8859-1"))

				String line
				while ((line = reader.readLine()) != null) {
					sb.append(line)
				}

				line = null
				is.close()
				reader = null
			}
			catch(e){
				System.out.println("ConnectToUrl - Url: " + urlProceso)
				System.out.println("ConnectToUrl - Ocurrio un error: " + e.printStackTrace())
				return null
			}

			connection.disconnect()
			connection = null
			webUrl = null
			is = null

			if(isProductive){
				proxy = null
			}

			if(sb.size() > 0)
			{
				source = sb.toString()
			}
		}

		if(source.contains("o foi encontrado")){
			System.out.println("CEP no encontrado: " + urlProceso)
			return "NOT_FOUND"
		}

		if(source.contains("Busca Inv") || source.contains("SESSAO EXPIRADA")){
			System.out.println("Busqueda invalidada o sesion expirada: " + urlProceso)
			return null
		}


		String correiosApiResult = null
		String zipCodeType = null

		try{

			int charIndex = 0
			String TipoCep = null

			// Procedo a extraer la info del detalle para saber que tipo de cep es:

			charIndex = source.indexOf("javascript:detalharCep")
			if(charIndex == -1){
				return null
			}
			TipoCep = source.substring(charIndex, source.size() -1)
			TipoCep = TipoCep.substring(28, 29)

			switch(TipoCep)
			{
				case "1": // LOC
				case "2": // LOG
				case "3": // PRO
				case "4": // CPC
				case "5": // GRU - UOP
					urlProceso = "http://www.buscacep.correios.com.br/servicos/dnec/detalheCEPAction.do?Metodo=detalhe&Posicao=1&TipoCep="+TipoCep+"&CEP="+ Cep
					break;
				default:
					System.out.println(Cep + " - El tipo de CEP no es valido: " + TipoCep)
					return null
			}

			source = null

			while(!source)
			{
				webUrl = new java.net.URL(urlProceso)
				CookieHandler.setDefault( new CookieManager( null, CookiePolicy.ACCEPT_ALL ) );

				connection = null
				proxy = null

				if(isProductive){
					proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("172.16.0.89", 80))
					connection = webUrl.openConnection(proxy)
				}else
				{
					connection = webUrl.openConnection()
				}

				try{
					connection.setRequestMethod("GET")
					connection.setRequestProperty("Accept-Charset", "ISO-8859-1");
					connection.setRequestProperty("Content-Type", "application/json; charset=ISO-8859-1")
					connection.setRequestProperty("Accept", "application/json")
					connection.setRequestProperty("Cookie", myCookie)
					connection.connect()

					is = connection.getInputStream()
					BufferedReader reader = new BufferedReader(new InputStreamReader(is, "ISO-8859-1"))

					String line
					while ((line = reader.readLine()) != null) {
						sb.append(line)
					}

					line = null
					is.close()
					reader = null
				}
				catch(e){
					System.out.println("ConnectToUrl - Url: " + urlProceso)
					System.out.println("ConnectToUrl - Ocurrio un error: " + e.printStackTrace())
					return null
				}

				connection.disconnect()
				connection = null
				webUrl = null
				is = null

				if(isProductive){
					proxy = null
				}

				if(sb.size() > 0)
				{
					source = sb.toString()
				}
			}

			if(source.contains("o foi encontrado")){
				System.out.println("CEP no encontrado: " + urlProceso)
				return null
			}

			if(source.contains("Busca Inv") || source.contains("SESSAO EXPIRADA")){
				System.out.println("Busqueda invalidada o sesion expirada: " + urlProceso)
				return null
			}

			def slurper = null
			def values = null
			int index = 0

			String cliente = null
			String logradouro = null
			String bairro = null
			String cidade = null
			String estado = null

			switch(TipoCep){
				case "5": // GRU: 01007-900 UOP: 88110-902
					zipCodeType = "770" // 770 - Grande Usuario GU - 4013178 - Unidad Operacional UO
					charIndex = source.indexOf('</table><table xmlns:fo="http://www.w3.org/1999/XSL/Format">')
					if(charIndex == -1){
						return null
					}

					correiosApiResult = source.substring(charIndex, source.size() -1)
					correiosApiResult = correiosApiResult.replace("</table><table xmlns:", "<table xmlns:")
					charIndex = correiosApiResult.indexOf('<br><table>')
					correiosApiResult = correiosApiResult.substring(0, charIndex)
					slurper = new XmlSlurper()
					values = slurper.parseText(correiosApiResult)

					values.tr[4].td[1] = values.tr[4].td[1].toString().replace("-","")
					if(values.tr[4].td[1].toString() == Cep){
						cliente = values.tr[0].td[1].toString()
						logradouro = values.tr[1].td[1].toString()
						bairro = values.tr[2].td[1].toString()
						cidade = values.tr[3].td[1].toString().split('/')[0]
						estado = values.tr[3].td[1].toString().split('/')[1]
					}

					if(estado == null || estado == ""){
						return null
					}

					if(cliente.contains("peracional")){
						zipCodeType = "4013178"
					}

					correiosApiResult = '{"cep":"'+Cep+'","type":"'+zipCodeType+'","cliente":"'+cliente+'","logradouro":"'+logradouro+'","bairro":"'+bairro+'","cidade":"'+cidade+'","estado":"'+estado+'"}'
					break;
				case "3": //05912-960
					zipCodeType = "772"
					charIndex = source.indexOf('<table border="1" cellpading="4" cellspacing="0" bordercolor="#FFFFFF" frame="hsides" rules="all" xmlns:fo="http://www.w3.org/1999/XSL/Format">')
					if(charIndex == -1){
						return null
					}
					correiosApiResult = source.substring(charIndex, source.size() -1)
					charIndex = correiosApiResult.indexOf('<br><table>')
					correiosApiResult = correiosApiResult.substring(0, charIndex)
					slurper = new XmlSlurper()
					values = slurper.parseText(correiosApiResult)

					values.tr[2].td[1] = values.tr[2].td[1].toString().replace("-","")
					if(values.tr[2].td[1].toString() == Cep){
						cliente = null
						logradouro = values.tr[0].td[1].toString()
						bairro = null
						cidade = values.tr[1].td[1].toString().split('/')[0]
						estado = values.tr[1].td[1].toString().split('/')[1]
					}

					if(estado == null || estado == ""){
						return null
					}
					correiosApiResult = '{"cep":"'+Cep+'","type":"'+zipCodeType+'","cliente":"'+cliente+'","logradouro":"'+logradouro+'","bairro":"'+bairro+'","cidade":"'+cidade+'","estado":"'+estado+'"}'
					break;
				case "4": //65580-990
					zipCodeType = "771" // 771 - Caja Postal CP
					charIndex = source.indexOf('<table border="1" cellpading="4" cellspacing="0" bordercolor="#FFFFFF" frame="hsides" rules="all" xmlns:fo="http://www.w3.org/1999/XSL/Format">')
					if(charIndex == -1){
						return null
					}
					correiosApiResult = source.substring(charIndex, source.size() -1)
					charIndex = correiosApiResult.indexOf('<br><table>')
					correiosApiResult = correiosApiResult.substring(0, charIndex)
					slurper = new XmlSlurper()
					values = slurper.parseText(correiosApiResult)

					values.tr[3].td[1] = values.tr[3].td[1].toString().replace("-","")
					if(values.tr[3].td[1].toString() == Cep){
						cliente = values.tr[0].td[1].toString()
						logradouro = values.tr[1].td[1].toString()
						bairro = null
						cidade = values.tr[2].td[1].toString().split('/')[0]
						estado = values.tr[2].td[1].toString().split('/')[1]
					}

					if(estado == null || estado == ""){
						return null
					}
					correiosApiResult = '{"cep":"'+Cep+'","type":"'+zipCodeType+'","cliente":"'+cliente+'","logradouro":"'+logradouro+'","bairro":"'+bairro+'","cidade":"'+cidade+'","estado":"'+estado+'"}'
					break;

				case "1": // LOC 89130-000
					zipCodeType = "773"
					charIndex = source.indexOf('<table border="0" xmlns:fo="http://www.w3.org/1999/XSL/Format">')
					if(charIndex == -1){
						return null
					}
					correiosApiResult = source.substring(charIndex, source.size() -1)
					charIndex = correiosApiResult.indexOf('<br><table>')
					correiosApiResult = correiosApiResult.substring(0, charIndex)
					slurper = new XmlSlurper()
					values = slurper.parseText(correiosApiResult)

					values.tr[2].td[1] = values.tr[2].td[1].toString().replace("-","")
					if(values.tr[2].td[1].toString() == Cep){
						cliente = null
						logradouro = null
						bairro = null
						cidade = values.tr[0].td[1].toString()
						estado = values.tr[1].td[1].toString()
					}

					if(estado == null || estado == ""){
						return null
					}

					correiosApiResult = '{"cep":"'+Cep+'","type":"'+zipCodeType+'","cliente":"'+cliente+'","logradouro":"'+logradouro+'","bairro":"'+bairro+'","cidade":"'+cidade+'","estado":"'+estado+'"}'
					break;

				case "2": // LOG 09330-148
					zipCodeType = "772"
					charIndex = source.indexOf('</table><table xmlns:fo="http://www.w3.org/1999/XSL/Format">')
					if(charIndex == -1){
						return null
					}
					correiosApiResult = source.substring(charIndex + 8, source.size() -1)
					charIndex = correiosApiResult.indexOf('<br><table>')
					correiosApiResult = correiosApiResult.substring(0, charIndex)
					slurper = new XmlSlurper()
					values = slurper.parseText(correiosApiResult)

					values.tr[3].td[1] = values.tr[3].td[1].toString().replace("-","")
					if(values.tr[3].td[1].toString() == Cep){
						cliente = null
						logradouro = values.tr[0].td[1].toString()
						bairro = values.tr[1].td[1].toString()
						cidade = values.tr[2].td[1].toString().split('/')[0]
						estado = values.tr[2].td[1].toString().split('/')[1]
					}

					if(estado == null || estado == ""){
						return null
					}

					correiosApiResult = '{"cep":"'+Cep+'","type":"'+zipCodeType+'","cliente":"'+cliente+'","logradouro":"'+logradouro+'","bairro":"'+bairro+'","cidade":"'+cidade+'","estado":"'+estado+'"}'
					break;
			}

			slurper = null
			values = null
		}
		catch(e){
			System.out.println("CEP: " +Cep+ " GetCorreiosInfo - Ocurrio un error: " + e.printStackTrace())
			System.out.println("Result: " +correiosApiResult)
		}

		zipCodeType = null
		source = null

		return correiosApiResult
	}

}
