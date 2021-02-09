package com.aliyun.hitsdb.client.util;

import java.text.ParseException;

public class WKTParser {
    public String rawString;
    public int offset;

    public WKTParser(String rawString) {
        this.rawString = rawString;
    }


    public String nextWord() {
        int startOffset;
        for(startOffset = this.offset; this.offset < this.rawString.length() && Character.isJavaIdentifierPart(this.rawString.charAt(this.offset)); ++this.offset) {
        }

        if (startOffset == this.offset) {
            return null;
        } else {
            String result = this.rawString.substring(startOffset, this.offset);
            this.nextIfWhitespace();
            return result;
        }
    }

    public double nextDouble() throws ParseException {
        int startOffset = this.offset;
        this.skipDouble();
        if (startOffset == this.offset) {
            throw new ParseException("Expected a number", this.offset);
        } else {
            double result;
            try {
                result = Double.parseDouble(this.rawString.substring(startOffset, this.offset));
            } catch (Exception var5) {
                throw new ParseException(var5.toString(), this.offset);
            }

            this.nextIfWhitespace();
            return result;
        }
    }

    public void skipDouble() {
        for(int startOffset = this.offset; this.offset < this.rawString.length(); ++this.offset) {
            char c = this.rawString.charAt(this.offset);
            if (!Character.isDigit(c) && c != '.' && c != '-' && c != '+' && (this.offset == startOffset || c != 'e' && c != 'E')) {
                break;
            }
        }

    }

    public void skipNextDoubles() {
        while(!this.eof()) {
            int startOffset = this.offset;
            this.skipDouble();
            if (startOffset == this.offset) {
                return;
            }

            this.nextIfWhitespace();
        }

    }

    public final boolean eof() {
        return this.offset >= this.rawString.length();
    }

    public void nextIfWhitespace() {
        while(this.offset < this.rawString.length()) {
            if (!Character.isWhitespace(this.rawString.charAt(this.offset))) {
                return;
            }

            ++this.offset;
        }

    }

    public void nextExpect(char expected) throws ParseException {
        if (this.eof()) {
            throw new ParseException("Expected [" + expected + "] found EOF", this.offset);
        } else {
            char c = this.rawString.charAt(this.offset);
            if (c != expected) {
                throw new ParseException("Expected [" + expected + "] found [" + c + "]", this.offset);
            } else {
                ++this.offset;
                this.nextIfWhitespace();
            }
        }
    }

    public void moveToExpect(char expected) throws ParseException {
        if (this.eof()) {
            throw new ParseException("Expected [" + expected + "] found EOF", this.offset);
        } else {
            for(; this.offset < this.rawString.length(); ++this.offset) {
                char c = this.rawString.charAt(this.offset);
                if (c == expected) {
                    ++this.offset;
                    break;
                }
            }
            if (this.offset >= this.rawString.length()) {
                throw new ParseException("not found Expected [" + expected + "]", this.offset);
            }
        }
    }
}
