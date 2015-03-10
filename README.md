# Scrape(grab) data from Maricopa AZ assessor.

script should take from command line following arguments:

type|t type of the search
version - default value = 2

before scrape starting script has to check if all necessary tables are
exist (<table>_<version>) and to create them if they are absent.

1. to go to
http://www.maricopa.gov/Assessor/ParcelApplication/Default.aspx
2. To perform search according to the <type>:
street = By Street # & Name:
subdivision = By Subdivision:

Search should be performed by 1 symbol from ASCII table.
If in search result is the message like "Your search criteria returned
1000+ results" number of ASCII symbols in search request should be
increased until getting less than 1k records.

If <type>=subdivision:
- to save records from search result page to
assessor_subdivisions_<version> table with MCR Number as primary key.
- to open page for each subdivision and for each parcel on that page to
open link like:
http://www.maricopa.gov/Assessor/ParcelApplication/DetailPrinterFriendl
y.aspx?ID=501-25-172

If <type>= street:
- for each parcel on search result page to open link like:
http://www.maricopa.gov/Assessor/ParcelApplication/DetailPrinterFriendl
y.aspx?ID=501-25-172

To scrape all data from parcel page to assessor_scrape_<version> with
Parcel #: as primary key
To set reference to subdivisions_<version> table if parcel has a
subdivision.

Valuation Information data should be saved to separate table with the
assessor_valuation_<version> with Parcel # as a key.

Parcels usually have same set of fields on the page but some of them
can differ. Scrape should create additionaly fields in
assessor_scrape_<version> automatically.

Note. It's old project. Origin source site structure was changed.
