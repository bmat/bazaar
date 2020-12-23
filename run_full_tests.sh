docker run -d --rm -p 27017:27017 --name "bazaar-test" mongo
python bazaar/test/test.py
docker stop bazaar-test