import unittest
from unittest.mock import patch
from epamRestaurant import fetch_geocodedata_byaddress, GEOCODER_KEY  # Replace with the actual import path


class TestGeocodingFunction(unittest.TestCase):

    @patch('epamRestaurant.fetch_geocodedata_byaddress')
    def test_fetch_geocodedata_byaddress(self, mock_get):
        # Define a fake response that the mock will return when the function calls requests.get
        mock_response = {
            "results": [
                {
                    "geometry": {
                        "location": {
                            "lat": 40.7128,  # Example latitude
                            "lng": -74.0060  # Example longitude
                        }
                    }
                }
            ]
        }

        # Configure the mock to return the fake response
        mock_get.return_value.json.return_value = mock_response

        # Call the function under test
        address = "New York, NY"
        lat, lng = fetch_geocodedata_byaddress(address)

        # Assert that the returned longitude and latitude are as expected
        self.assertEqual(lng, -74.0060152)
        self.assertEqual(lat, 40.7127281)


if __name__ == '__main__':
    unittest.main()