docker run -d --rm -p 27017:27017 --name "bazaar-test" mongo
MONGO_URI="mongodb://localhost/bazaar_test" python bazaar/test/test.py
docker stop bazaar-test