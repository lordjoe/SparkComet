package com.lordjoe.distributed.input;

public class MGFSpecialDelimitedText extends SpecialDelimitedTextFormat {
    @Override
    public String getDelimiterText() {
        return "BEGIN IONS";
    }
}
