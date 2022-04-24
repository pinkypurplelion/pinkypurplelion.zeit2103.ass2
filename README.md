---
title: Technical Report - Assignment 2
author: z5414201
date: "2022-04-24"
geometry: margin=2cm
titlepage: true
header-left: ZEIT2103
header-center: Technical Report
header-right: Assignment 2
listings-no-page-break: true
---

# Task 1

## Questions

**For the Athletes table, would using name as the primary key be acceptable?**

Not necessarily. Whilst the chance that two athletes have the same name is low, it is possible. Hence the name couldn't be used solely as the primary key because that would result in only one athlete with that name being able to be present in the database.

<hr> 

**For the Medals table, is there an alternative (possibly composite) primary key that could be used rather than having an explicit row ID? If so, what benefits would using this composite key have?**

Alternative option for primary key: `olympicID + eventID + athleteID`. The alternate cannot be `olympicID + eventID + medalColour` because in the event that two or more athletes were to formally draw, each athlete should be awarded a medal of the same colour. Using a composite key would ensure that duplicate medals are not added to the DB by accident. When using the ID PK, multiple rows may reference the same Olympic medal.

<hr>

**Why is 3NF desirable in a database?**

In Third Normal Form (3NF), all transitive functional dependencies have been removed. This means that each column is only dependent on columns that are a part of the primary key. This results in redundancy within the tables being reduced. A reduction in redundancy is helpful because it makes it easier to change data, since it only appears in one place within the database. It also helps with search efficiency due to the inherent requirement of smaller tables when data is normalised. Finally, 3NF should also improve overall data quality since there is less of a chance unwanted data will be stored in the tables by requirement.

# Task 2

# Task 3