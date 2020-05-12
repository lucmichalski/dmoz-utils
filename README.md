dmozscraper
===========

This collection of tools has the purpose of going through the [Open Directory Project](http://dmoz.org) content data dump and extracting data.

It includes:
* a tool that extracts data from the [content rdf dump](http://www.dmoz.org/rdf.html) and produces a CSV
* a tool that will take the CSV and load up jobs in a redis queue
* a tool that will pop a job off a redis queue and scrape the urls for content and stores it in a postgres datagbase


Requirements
------------
* go programming language
* redis
* postgresql
* git
* mercurial
* go get github.com/vmihailenco/redis
* go get github.com/bmizerany/pq
* go get github.com/saintfish/chardet
* go get code.google.com/p/go-charset/charset
* go get code.google.com/p/go-charset/data

Libraries used:
--------------
* https://github.com/vmihailenco/redis
* https://github.com/bmizerany/pq
* https://github.com/saintfish/chardet
* https://code.google.com/p/go-charset/
