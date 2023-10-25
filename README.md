# ETL-algorithm
<p align="justify">
We will be exploring a type of batch processing called Extract, Transform and Load (ETL). Extract, Transform and Load or (ETL) does exactly what the name implies. It is the process of extracting large amounts of data from multiple sources and formats and transforming it into one specific format before loading it into a database or target file.
</p>
<p align="center">
<img width="500" src="https://user-images.githubusercontent.com/70657426/148559191-080feef7-1a54-42ce-a04b-cf7cc56ea855.jpg">
</p>

<p align="justify">
In this project, we will generate a pipeline to extract cars dealership data from files of three different types: JSON, XML and CSV. The data sources are available in the dealership data folder above and contain four columns of interest, the car model, the year of manufacture, its price, and the type of fuel.
</p>
<br> 
<p align="center">
<img width="600" src= "https://user-images.githubusercontent.com/70657426/148559640-87e35288-c09a-408d-9a57-6de814b6a71b.png">
</p>
<p align="justify">
After extracting the data from each file and appending them to one data frame, we will transform the desired columns and standardise them before loading our new dataframe into a new file. The new file generated is now ready for any analysis using any kind of Business Intelligence tools.
</p>
