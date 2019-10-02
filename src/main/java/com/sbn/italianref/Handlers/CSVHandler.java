package com.sbn.italianref.Handlers;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;


import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class CSVHandler {

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

    public static void write(Path pathName, List<String[]> rows) {
        try {
            Writer writer = Files.newBufferedWriter(pathName);
            CSVWriter csvWriter = new CSVWriter(writer,
                    CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.NO_QUOTE_CHARACTER,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END);
            csvWriter.writeAll(rows);
            csvWriter.close();
            writer.close();
        } catch (IOException e) {
            System.err.println("Error writing csv: "+e.getMessage());
        }
    }
}
