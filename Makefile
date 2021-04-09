all: zengin/zengindata.json

clean:
	rm raw/*
	rm zengin/zengindata.json

raw/ginkositen1.zip:
	wget -O raw/ginkositen1.zip 'http://ykaku.com/ginkokensaku/ginkositen1.zip'

raw/ginkositen.utf8.csv: raw/ginkositen1.zip
	cd raw; \
	unzip -o ginkositen1.zip
	iconv -f sjis -t utf8 raw/ginkositen1.txt > raw/ginkositen.utf8.csv

zengin/zengindata.json: raw/ginkositen.utf8.csv
	python zengin/prep.py
