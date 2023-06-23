package org.example

// read from csv

// NEED TO ADD THE FOLLOWING TO THE GRADLE FILE ;  with configuration will not work without further refactoring to align with 
//      Java standards (e.g. src/app/java/...)
// implementation 'com.opencsv:opencsv:5.5'


import com.opencsv.CSVReaderHeaderAware
import java.io.FileReader

fun main() {
    val reader = CSVReaderHeaderAware(FileReader("../../MOCK_DATA.csv"))
    val resultList = mutableListOf<Map<String, String>>()
    var line = reader.readMap()
    while (line != null) {
        resultList.add(line)
        line = reader.readMap()
    }
    println(resultList)
    // Line 2, by column name
    println(resultList[1]["my column name"])
}