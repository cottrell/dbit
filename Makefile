all: python

VERSION=`cat deribit-python/VERSION`

python:
	docker build deribit-python -t cottrell/deribit-python:latest
	docker tag cottrell/deribit-python:latest cottrell/deribit-python:$(VERSION)

.PHONY: test
test:
	echo $(VERSION)
