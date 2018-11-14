all: python

VERSION=`cat deribit-python/VERSION`

python:
	# cp do.py lib.py deribit-python
	docker build deribit-python -t cottrell/deribit-python:latest
	docker tag cottrell/deribit-python:latest cottrell/deribit-python:$(VERSION)

.PHONY: test
test:
	echo $(VERSION)
