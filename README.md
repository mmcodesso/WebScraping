# WebScraping
This repsoitory contains some code to get data from a website with javascript 

- The file webscraperjs.py  uses the library requests-html to get the information form a website with javascript, it will render the page and get the value.
The output file is a sqlite3 file for easy query and uses a multiprocessing with pathos library.
it will process with chunksizes

- The file selenium_downloader.py use the same idea, however it uses selenium.

- Docker: I added the files to run the same script usinf docker and selenium, try this if you have some problens to run on you enviroment
