# **smonitor**

[![Build Status](https://travis-ci.org/skgrange/smonitor.svg?branch=master)](https://travis-ci.org/skgrange/smonitor)

## Introduction

**smonitor** is an R package which contains a collection of functions which allow me to maintain air quality monitoring data. **smonitor** will only work out-of-the-box with a specific, but simple data model (also known as a schema). This data model is best implemented with a SQL database. Although the development reflects the management of air quality data, the functions and data model should be utilitarian enough to be applied to other time-series measurements. 

## Installation

The development version: 
```
# Install dependency
devtools::install_github("skgrange/threadr")

# Install smonitor
devtools::install_github("skgrange/smonitor")
```

## Background

I have been involved with many projects recently which have seriously complicated the storage and retrieval of simple time-series data. I believe that time-series data is very simple; at a fundamental level there are observations in time and space which need to be stored. In my experience, most of the complication arises when:

  - Many time-series are turned on and off. 
  - There are introductions of temporally overlapping time-series. This occurs when multiple sensors monitoring the same variable are located at a single monitoring site, such as two NO<sub>x</sub> analysers.
  - A number of aggregations need to be calculated. Aggregations often have dependence on other aggregations and although the majority of aggregations are simple, there are a few which are rather tricky. 
  
**smonitor** attempts to provide a framework and functions so these things can be dealt with easily. 

### Goals

The primary goals of **smonitor** are: 

  - Provide a simple data model for time-series data. The data model should also allow for extensions. 
  - Provide functions to calculate aggregations which can be easily scheduled and be dynamic so they reflect source data changes. 
  - Provide importing functions so data can be imported and used easily and conveniently for analysis. 
    - This is the end-goal and the key reason why this development was started, accessing data should be easy! 

## The data model

### A `process`

The **smonitor**'s data model uses generic nouns and verbs to keep things portable. The primary identifier for a location-variable pair is called the `process`. A `process` is best described as an unique and usually a mostly uninterrupted time-series. For example, a temperature sensor at a monitoring site for several years would represent a single process. If relative humidity and pressure were also monitored at the same location, they would form other processes as would other variables at other monitoring sites/locations. However, if the original temperature sensor failed and was replaced, the replacement would represent a new process. Changes in instrumentation would usually give cause to create a new process, but if instrumentation contains consumables, such as BAMs (beta attenuation monitors) with their filter-tape, new processes could be used to represent these changes too; but only if this was desired. 

The current data model is implemented with six tables: 

  - `processes`: Stores information of unique time-series. `processes` contains keys to join all other tables together and is the main mapping table. 
  - `sites`: Stores information of monitoring locations/facilities such as names, identifiers, addresses, and coordinates. This table can be a spatial-table. 
  - `aggregations`: Stores information of aggregation functions and methods.
  - `summaries`: Stores information of what aggregations should be preformed on processes. 
  - `invalidation`: Stores date ranges where a process is considered invalid. An optional component and is used only when source data obviously contains errors. 
  - `observations`: Stores measurement data as well as the aggregations of measurement data.
