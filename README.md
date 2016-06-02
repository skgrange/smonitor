# **smonitor**

[![Build Status](https://travis-ci.org/skgrange/smonitor.svg?branch=master)](https://travis-ci.org/skgrange/smonitor)

## Introduction

**smonitor** is both a framework and a collection of functions which operate on this framework to allow for maintenance of air quality monitoring data. **smonitor** is an R package which services the specific, but simple data model (also known as a schema) implemented with a SQL database. Although the development reflects the management of air quality monitoring data, the functions and data model are utilitarian enough to be applied to other time-series measurements. 

## Installation

The development version: 
```
# Load package
library(devtools)

# Install dependencies
install_github("skgrange/threadr")
install_github("davidcarslaw/worldmet")

# Install smonitor
install_github("skgrange/smonitor")
```

A minimal SQLite example of the database can be found at `inst/extdata/smonitor_example.db`. The addition of other variables/columns/fields in the tables should not be a problem, but the removal of the core variables is not recommended if the R functions within this package are to be used without modification. 

### Background

I have been involved with many projects recently which have seriously complicated the storage and retrieval of simple time-series data, an example can be found [here](https://wiki.52north.org/bin/view/SensorWeb/SosDataModeling#The_52_North_SOS_standard_data_m). I believe that time-series data is very simple; at a fundamental level there are observations in time and space which need to be stored. In my experience, most of the complication arises when:

  - Many time-series are turned on and off. 
  - There are introductions of temporally overlapping time-series. This occurs when multiple sensors monitoring the same variable are located at a single monitoring site. For example for a season, a number of NO<sub>x</sub> analysers operate at the same monitoring site. 
  - A number of aggregations need to be calculated. Aggregations often have dependence on other aggregations and although the majority of aggregations are simple, there are a few which are rather tricky. 
  
**smonitor** attempts to provide a framework and functions so these things can be dealt with easily and getting data into and out of a database is simple and fast. 

## Objectives

The primary objectives of **smonitor** are: 

  - Provide a simple core data model for time-series data. The data model should allow for extensions for specific uses. 
  - Provide functions to insert data and calculate aggregations which can be easily scheduled and be dynamic. This allows the database to reflect source data changes which occurs constantly with time-series measurements. 
  - Provide importing functions so data can be imported and used easily and conveniently for analysis and data delivery. 
    - This is the end-goal and the key reason why this development was started, accessing good quality data should be easy! 

## The data model

The current data model is implemented with six tables: 

  - `processes`: Stores information of unique time-series. `processes` contains keys to join all other tables together and is the main mapping table. 
  - `sites`: Stores information of monitoring locations/facilities such as names, identifiers, addresses, and coordinates. This table can be a spatial-table. 
  - `aggregations`: Stores information of aggregation functions and methods.
  - `summaries`: Stores information of what aggregations should be preformed on processes. 
  - `invalidations`: Stores date ranges where a process is considered invalid. An optional component and is used only when source data obviously contains errors. 
  - `observations`: Stores measurement data as well as the aggregations of measurement data.

An entity-relationship diagram of the core data model looks like this:

![**smonitor**'s core data model](inst/extdata/smonitor_entity_relationship_diagram.png)

The data model has some constraints, but by design they have been used sparingly.

### A `process`

The **smonitor**'s data model uses generic nouns and verbs to keep things portable. The primary identifier for a location-variable pair is called the `process`. A `process` is best described as an unique, and usually, an un-interrupted time-series. For example, a temperature sensor at a monitoring site for several years would represent a single process. If relative humidity and pressure were also monitored at the same location, they would form other processes. Other variables at other monitoring sites/locations would also be additional processes. However, if the original temperature sensor failed and was replaced, the replacement would be a new process. Changes in instrumentation would usually give cause to create a new process, but if instrumentation contains consumables, such as BAMs (beta attenuation monitors) with their filter-tape, new processes could be used to represent these changes too; but only if this was desired. A process can be as granular or specific as a user needs it to be. 

## Uses

The **smonitor** database is used for many pieces of my work including: 

  - My personal homepage ([here](http://skgrange.github.io/temperature_plots.html) and [here](http://skgrange.github.io/air_quality_plots.html))

