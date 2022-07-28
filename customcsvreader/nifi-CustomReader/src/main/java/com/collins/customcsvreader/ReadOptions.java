package com.collins.customcsvreader;

//Options for the cdr record reader
public class ReadOptions
{
    private boolean skipMalformedLines;

    public boolean skipMalformedLines(){
        return skipMalformedLines;
    }

    public ReadOptions skipMalformedLines(final boolean skip){
        skipMalformedLines = skip;
        return this;
    }
}