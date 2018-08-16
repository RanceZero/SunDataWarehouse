# This is how it would query sunpy the data mapped in 'gathering_data_information.txt'
# At this time the vso client seems unstable and sunpy integration could not be complete

from sunpy.database import Database
from sunpy.net import vso

#Sets the connection to database, in this case, it is in postgresql but it can be others too
database = Database('postgresql+psycopg2://user:password@host/dbname')

#downloads and add to database
def downloadData(q_provider, q_source, q_instrument, q_physobs):
	client = vso.VSOClient()
	qr = client.search(
		vso.attrs.Time('2016-05-07 00:00:00', '2016-05-08 00:00:00'))
	print(qr)
	database.add_from_vso_query_result(qr)
	database.commit()

#downloads MAgnetogram vector from vso client
def dl_MagnetogramVector():
	q_provider = 'JSOC'
	q_source = 'SDO'
	q_instrument = 'HMI'
	q_physobs = 'vector_magnetic_field'
	downloadData(q_provider, q_source, q_instrument, q_physobs)

#downloads XRay data from vso client
def dl_XRay():
	q_provider = 'NGDC'
	q_source = 'GOES-12'
	q_instrument = 'SXI-0'
	q_physobs = 'intensity'
	downloadData(q_provider, q_source, q_instrument, q_physobs)

dl_MagnetogramVector()
dl_XRay()
