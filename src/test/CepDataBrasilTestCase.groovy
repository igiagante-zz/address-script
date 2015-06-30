package test

import services.ZipcodesServiceStream
import spock.lang.Specification

/**
 * Created by igiagante on 12/5/15.
 */
class CepDataBrasilTestCase extends Specification {

    def "testing cep 01007-900"() {

        when:
        Map<String, String> map = ZipcodesServiceStream.getCepData("01007900")

        then:
        assert map.get("Cliente") == "Edifício Conde Francisco Matarazzo"
        assert map.get("Endereço") == "Rua Doutor Faria Pereira, 56"
        assert map.get("Bairro") == "Centro"
        assert map.get("Localidade") == "São Paulo/SP"
        assert map.get("CEP") == "01007-900"
    }

    def "testing cep 05912-960"() {

        when:
        Map<String, String> map = ZipcodesServiceStream.getCepData("05912960")

        then:
        assert map.get("Promoção") == "Palavra Misteriosa"
        assert map.get("Localidade") == "São Paulo/SP"
        assert map.get("CEP") == "05912-960"
        assert map.get("Abrangência") == ""
    }

    def "testing cep 89130-000"() {

        when:
        Map<String, String> map = ZipcodesServiceStream.getCepData("89130000")

        then:
        assert map.get("Localidade") == "Indaial"
        assert map.get("CEP") == "89130-000"
    }

    def "testing cep 09330-148"() {

        when:
        Map<String, String> map = ZipcodesServiceStream.getCepData("09330148")

        then:
        println map
        assert map.get("Logradouro") == "Viela Dina Silva dos Santos"
        assert map.get("Bairro") == "Jardim Itapeva"
        assert map.get("Localidade") == "Mauá/SP"
        assert map.get("CEP") == "09330-148"
    }

    def "testing cep 88110-902"() {

        when:
        Map<String, String> map = ZipcodesServiceStream.getCepData("88110902")

        then:
        assert map.get("Cliente") == "COA - Centro Operacional e Administrativo dos Correios"
        assert map.get("Endereço") == "Rua Romeu José Vieira, 90"
        assert map.get("Bairro") == "Nossa Senhora do Rosário"
        assert map.get("Localidade") == "São José/SC"
        assert map.get("CEP") == "88110-902"
    }

    def "testing cep 65580-990"() {

        when:
        Map<String, String> map = ZipcodesServiceStream.getCepData("65580990")

        then:
        assert map.get("CPC") == "Associação Comunitária dos Moradores do Comum"
        assert map.get("Endereço") == "Comum - Sítio União"
        assert map.get("Localidade") == "Tutóia/MA"
        assert map.get("CEP") == "65580-990"
    }
}