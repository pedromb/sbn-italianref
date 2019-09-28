package com.sbn.italianref;

import org.apache.commons.lang3.StringUtils;
import java.util.StringTokenizer;

public class ProcessText {


    public static String processText(String text) {

        String auxString = text
                .replaceAll("https?://\\S+\\s?", "");
        StringTokenizer tokenizer = new StringTokenizer(auxString, " ");
        StringBuilder builder = new StringBuilder();
        while(tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            if(token.length() >= 2){
                token = token.replaceAll("\\P{L}", "");
                builder.append(token);
                builder.append(" ");
            }
        }
        String result = builder
                .toString()
                .trim()
                .replaceAll(" +", " ")
                .toLowerCase();
        result = StringUtils.stripAccents(result);
        return result;

    }
}
