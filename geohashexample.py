import geohash

latitude = 40.7128   # Example: New York City
longitude = -74.0060

# Encode into geohash with 4-character precision
geohash_value = geohash.encode(latitude, longitude, precision=4)
print(f"Geohash: {geohash_value}")