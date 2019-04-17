package com.lordjoe.distributed.input;

public class FastaSpecialDelimitedText extends SpecialDelimitedTextFormat {
    @Override
    public String getDelimiterText() {
        return ">";
    }
}
