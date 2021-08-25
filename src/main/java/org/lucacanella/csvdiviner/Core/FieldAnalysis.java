package org.lucacanella.csvdiviner.Core;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.regex.Pattern;

public class FieldAnalysis {

    public static final int MAX_SAMPLES   = 40;

    //public static final Pattern nonDigitsP = Pattern.compile("[\\D]");
    public static final Pattern intP = Pattern.compile("^(0)|(-?[1-9][0-9]*)$");
    public static final Pattern floatP = Pattern.compile("^-?[0-9]+(\\.|,)[0-9]+$");
    public static final Pattern dateP = Pattern.compile("^([0-9]{2}/[0-9]{2}/[0-9]{4})|([0-9]{4}-[0-9]{2}-[0-9]{2})$");
    public static final Pattern dateTimeP = Pattern.compile("^(([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})|([0-9]{4}-[0-9]{1,2}-[0-9]{1,2}))((T|\\s)?[0-9]{1,2}:[0-9]{1,2}:?[0-9]{0,2})((\\.|\\s)[0-9]{0,3})?$");

    int index;
    String header;

    int maxStrLen;
    int minStrLen;
    boolean hasValues;
    boolean hasNulls;
    boolean containsFloats;
    boolean containsIntegers;
    boolean containsDate;
    boolean containsDateTime;
    boolean containsOtherValues;
    transient Set<String> stringSamples;
    transient Set<String> integerSamples;
    transient Set<String> floatSamples;
    transient Set<String> dateSamples;
    transient Set<String> datetimeSamples;

    String[] Strings;
    String[] Integers;
    String[] Floats;
    String[] Dates;
    String[] Datetimes;

    int samplesCount;

    transient boolean analysisFinalized;

    public int getIndex() {
        return index;
    }

    public String getHeader() {
        return header;
    }

    public int getMaxStrLen() {
        return maxStrLen;
    }

    public int getMinStrLen() {
        return minStrLen;
    }

    public boolean hasValues() {
        return hasValues;
    }

    public boolean hasNulls() {
        return hasNulls;
    }

    public boolean containsFloats() {
        return containsFloats;
    }

    public boolean containsIntegers() {
        return containsIntegers;
    }

    public boolean containsDate() {
        return containsDate;
    }

    public boolean containsDateTime() {
        return containsDateTime;
    }

    public boolean containsOtherValues() {
        return containsOtherValues;
    }

    public Set<String> getStringSamples() {
        return stringSamples;
    }

    public Set<String> getIntegerSamples() {
        return integerSamples;
    }

    public Set<String> getFloatSamples() {
        return floatSamples;
    }

    public Set<String> getDateSamples() {
        return dateSamples;
    }

    public Set<String> getDatetimeSamples() {
        return datetimeSamples;
    }

    public int getSamplesCount() {
        return samplesCount;
    }

    public boolean isAnalysisFinalized() {
        return analysisFinalized;
    }

    public FieldAnalysis finalizeBeforeSerialize() {
        Strings = getSamples(stringSamples);
        Integers = getSamples(integerSamples);
        Floats = getSamples(floatSamples);
        Dates = getSamples(dateSamples);
        Datetimes = getSamples(datetimeSamples);
        analysisFinalized = true;
        return this;
    }

    public String[] getSamples(Set<String> smplSet) {
        LinkedList<String> samplesList = new LinkedList<String>();
        int samplesCount = 0;
        for(String x : smplSet) {
            samplesList.add(x);
            ++samplesCount;
            if(samplesCount >= MAX_SAMPLES) {
                break;
            }
        }
        return samplesList.toArray(new String[samplesCount]);
    }

    public FieldAnalysis(String header, int index) {
        this.index = index;
        this.header = header;
        this.hasValues = false;
        this.hasNulls = false;
        this.maxStrLen = 0;
        this.minStrLen = Integer.MAX_VALUE;
        this.containsIntegers = false;
        this.containsDate = false;
        this.containsDateTime = false;
        this.containsFloats = false;
        this.containsOtherValues = false;
        this.stringSamples = new HashSet<String>();
        this.integerSamples = new HashSet<String>();
        this.floatSamples = new HashSet<String>();
        this.dateSamples = new HashSet<String>();
        this.datetimeSamples = new HashSet<String>();
        this.samplesCount = 0;
        this.analysisFinalized = false;
    }

    public void mergeType(FieldAnalysis other) {
        if(header != other.header) {
            throw new IllegalStateException(String.format("Header mismatch %s <> %s", header, other.header));
        }

        hasValues = hasValues | other.hasValues;
        hasNulls = hasNulls | other.hasNulls;
        maxStrLen = Math.max(maxStrLen, other.maxStrLen);
        minStrLen = Math.min(minStrLen, other.minStrLen);
        containsIntegers = containsIntegers | other.containsIntegers;
        containsDate = containsDate | other.containsDate;
        containsDateTime = containsDateTime | other.containsDateTime;
        containsOtherValues = containsOtherValues | other.containsOtherValues;

        stringSamples.addAll(other.stringSamples);
        integerSamples.addAll(other.integerSamples);
        floatSamples.addAll(other.floatSamples);
        dateSamples.addAll(other.dateSamples);
        datetimeSamples.addAll(other.datetimeSamples);

        this.samplesCount =
                stringSamples.size()
                + integerSamples.size()
                + floatSamples.size()
                + dateSamples.size()
                + datetimeSamples.size()
        ;
    }

    public void evaluate(String data, int colIdx) {
		/*if(colIdx != index) {
			throw new RuntimeException(String.format("Headers mismatch: %d <> %d", colIdx, index));
		}*/
        if(null != data) {
            int len = data.length();
            if(len == 0) {
                hasNulls = true;
            } else {
                hasValues = true;
                maxStrLen = Math.max(maxStrLen, len);
                minStrLen = Math.min(minStrLen, len);

                boolean intMatch = intP.matcher(data).matches();
                boolean floatMatch = floatP.matcher(data).matches();
                boolean dateMatch = dateP.matcher(data).matches();
                boolean datetimeMatch = dateTimeP.matcher(data).matches();
                boolean otherValues = !(intMatch|floatMatch|dateMatch|datetimeMatch);

                containsIntegers |= intMatch;
                containsFloats |= floatMatch;
                containsDate |= dateMatch;
                containsDateTime |= datetimeMatch;
                containsOtherValues |= otherValues;

                if(intMatch && integerSamples.size() < MAX_SAMPLES) {
                    integerSamples.add(data);
                } else if(floatMatch && floatSamples.size() < MAX_SAMPLES) {
                    floatSamples.add(data);
                } else if(dateMatch && dateSamples.size() < MAX_SAMPLES) {
                    dateSamples.add(data);
                } else if(datetimeMatch && datetimeSamples.size() < MAX_SAMPLES) {
                    datetimeSamples.add(data);
                } else if(otherValues && stringSamples.size() < MAX_SAMPLES) {
                    stringSamples.add(data);
                }
            }
        } else {
            hasNulls = true;
        }
    }
}