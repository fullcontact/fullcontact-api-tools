@Grapes( 
    @Grab(group='net.sf.opencsv', module='opencsv', version='2.3') 
)

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import static java.net.URLEncoder.encode
import groovy.json.JsonSlurper
import groovy.util.CliBuilder


def cli = new CliBuilder(usage: """Enriches a location field in a CSV file and 
appends information to the CSV, writing to stdout.

Usage:
	${this.class.simpleName}.groovy -k [API Key] -f [field] [input csv] > my_output.csv
""")

cli.k(longOpt: 'key', args: 1, argName: 'apiKey', 'API Key')
cli.f(longOpt: 'field', args: 1, argName: 'field', 'Field index in the CSV (0-based)')
def options = cli.parse(args)
if (!options.k || !options.arguments() || !options.f) {
	System.err.println cli.usage
	System.exit(1)
}

def enrichmentBaseUrl = 'https://api.fullcontact.com/v2/address/locationEnrichment.json?place='
def normalizerBaseUrl = 'https://api.fullcontact.com/v2/address/locationNormalizer.json?place='
def apiKey = options.k
def inFile = options.arguments().first()
def field = Integer.parseInt(options.f)

CSVReader reader = new CSVReader(new FileReader(inFile));
String[] nextLine;

// skip header line
def firstLine = reader.readNext()

System.out.withWriter{outwriter ->
	def writer = new CSVWriter(outwriter)
	def header = (firstLine + ['Response Code', 'City', 'County', 'State Name', 'State Code', 'Country Name', 'Country Code', 'Continent', 'Population']) as String[]
	writer.writeNext(header)

	def count = 0
	while ((nextLine = reader.readNext()) != null) {
	    if (nextLine.length < field) continue;
    
	    def location = nextLine[field]
	
		def normalUrl = normalizerBaseUrl + java.net.URLEncoder.encode(location) + "&apiKey=" + java.net.URLEncoder.encode(apiKey)

	    def output = nextLine.toList()
	    try {
		    def conn = normalUrl.toURL().openConnection()
			def status = conn.getResponseCode()
        	if (status == 200) {
				def normal = new JsonSlurper().parseText(conn.inputStream.text)
				def normalizedLocation = normal.normalizedLocation
			    def url = enrichmentBaseUrl + java.net.URLEncoder.encode(normalizedLocation) + "&apiKey=" + java.net.URLEncoder.encode(apiKey)
				conn = url.toURL().openConnection()
				status = conn.getResponseCode()
        
		        output << status
		        if (status == 200) {
			        def response = new JsonSlurper().parseText(conn.inputStream.text)
		            if (response.locations) {
		                def loc = response.locations[0]
		                output << loc.city
		                output << loc.county
		                output << loc.state?.name
		                output << loc.state?.code
		                output << loc.country?.name
		                output << loc.country?.code
		                output << loc.continent
		                output << loc.population
		            }
		        }
			} else {
				output << status
			}
	    } catch (java.io.IOException ex) {
	        if (ex.message.contains("422"))
	            output << "422"        
	        else if (ex.message.contains("404"))
	            output << "404"
	        else {
	            output << "500"
	        }
	    }

		System.err.println(++count)
	    writer.writeNext(output as String[])
		writer.flush()
	}
}
