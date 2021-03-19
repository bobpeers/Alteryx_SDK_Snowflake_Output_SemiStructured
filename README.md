# Alteryx SDK Snowflake Output Semi-Structured Data


# Description
Alteryx tool to output Semi-Structuted data to Snowflake. This includes:

- JSON
- XML
- PARQUET
- AVRO
- ORC

The tools allows for:

- Creating tables (drop if already exists) and appending data
- Truncate data in exisiting table and appending
- Append data to an existing table

Tables are created accoding to the [Snowflake Documentation](https://docs.snowflake.com/en/user-guide/semistructured-intro.html) using the **VARIANT** data type. An exmpale of this SQL is shown below.

```sql
create or replace table mytable (column_name VARIANT)
```

## Input Files

All the input files **MUST** be of the same type. The connector checks the file extentions of all the files and will stop with an error is there is more than one file extension found.

The files must also have a file extention of type **json, xml, parquet, avro, orc**. Any other files will stop the connector with an error.

It is up to you to only load files with the same schema, the connector will **NOT** check the schemas are the same and will load them regardless.

## Advanced Options Include
- Quote all fields (they will be case sensitive in Snowflake)
- Suspend the warehouse immediately after running (this will cause Snowflake to wait until current operations are finished first)

## Installation
Download the yxi file and double click to install in Alteryx. 

<img src="https://user-images.githubusercontent.com/4363445/111751520-e5fc8100-8894-11eb-99ef-9bd7ff444d30.png" width='500px' alt="Snowflake Install Dialog">

The tool will be installed in the __Connectors__ category.

## Requirements

The tool installs the official [Snowflake Connector library](https://docs.snowflake.com/en/user-guide/python-connector.html). If you have already installed another Snowflake SDK tool from me then it will share the same Python libraries as the other connectors.

## Authorisation
This can be either via Snowflake or Okta. If you select Okta authentication this must be set up on the server according to the [Snowflake Instructions](https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-configure-snowflake.html). 

<img src='https://github.com/bobpeers/Alteryx_SDK_Snowflake_Output/blob/main/images/okta.gif' width=500px alt='Snowflake Okta Authentication'>

## Usage

The tool must be fed a list of full paths to the files you wish to upload. The easiest way to do this is to use a Directory tool to read the files and then map the **FullPath** field to the tool.

The files will then be loaded to the chosen table.

<img src='https://user-images.githubusercontent.com/4363445/111752571-29a3ba80-8896-11eb-8ba7-198cf6a02ea4.png' alt='Example workflow'>

| ⚠️ Column name in Semi-Structured Snowflake Table|
|:---|
|The connector will use the column name containing the file paths as the column name in the Snowflake table so if you are not creating a new table this mush of course match with the column name in Snowflake|

## Configuration
Configure the tool using the setting for you Snowflake instance. Note that the account is the string to the left of __snowflakecomputing.com__ in your URL.

If you do not select a temporary path then the tool will use the default Alteryx temp path. Using this path the tool will create subfolders based on the current UNIX time.

| ⚠️ Note on Auto Suspending|
|:---|
|To automatically suspend the warehouse after running your user must have OPERATE permisions on the warehouse|

### Preserve Case Checkbox
If you don't select the preserve case option then the fields will be created as provided by the upstream tool. These fields will be checked for validity and if found to be invalid they will automatically be quested so thet become case sensitive in Snowflake. This setting also applies to table names.

## Logging
The tool will create log files for each run in the temp folder supplied. These logs contain detailed information on the Snowflake connection and can be used in case of unexected errors.

## Outputs
The tool has no output.

## Snowflake View

The JSON (or XML,...) data in Snowflake will look like this:

![image](https://user-images.githubusercontent.com/4363445/111756281-53f77700-889a-11eb-9ead-619207799c4c.png)

It can be quesried directly in Snowflake using the following syntax for JSON:

![image](https://user-images.githubusercontent.com/4363445/111756508-928d3180-889a-11eb-84ef-1d8a7dcd6bc8.png)


