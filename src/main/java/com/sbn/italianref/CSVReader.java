package com.sbn.italianref;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CSVReader {

    public static List<String[]> read(Path pathName, boolean hasHeader) {
        int skipLines = hasHeader ? 1 : 0;
        Reader reader = null;
        try {
            reader = Files.newBufferedReader(pathName);
            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withIgnoreQuotations(true)
                    .build();
            com.opencsv.CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withSkipLines(skipLines)
                    .withCSVParser(parser)
                    .build();
            List<String[]> list = csvReader.readAll();
            reader.close();
            csvReader.close();
            return list;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
