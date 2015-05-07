import json
from pprint import pprint
import urllib2


def find_add(address):
	status=0
	api_key='AIzaSyC_q-2y1dJ0naYwdu1CT_ExqDRQ95rsTrY'
	address='https://maps.googleapis.com/maps/api/geocode/json?address='+address+'&key='+api_key
	response = urllib2.urlopen(address)
	data = json.load(response)
	format_add=''
	lat=''
	lng=''
	if data["status"]=='OK':
		status=1
		n=len(data["results"])
		print "Total addresses found :",n
		format_add = data["results"][0]["formatted_address"]
		lat = data["results"][0]["geometry"]["location"]["lat"]
		lng = data["results"][0]["geometry"]["location"]["lng"]
	return format_add,lat,lng,status
