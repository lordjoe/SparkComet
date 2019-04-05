package com.lordjoe.comet;


import com.lordjoe.distributed.XMLUtilities;

import java.io.Serializable;
import java.util.*;

/**
 * com.lordjoe.comet.SpectrumQueryWithHits
 * User: Steve
 * Date: 12/14/2018
 */
public class SpectrumQueryWithHits implements Serializable {


    public final String queryLine;
    private final Set<String> searchHits;


    public SpectrumQueryWithHits(String wholeQuery) {
        queryLine = wholeQuery.substring(0,wholeQuery.indexOf("\n")).trim();
        List<String> hits = XMLUtilities.extractXMLTags(wholeQuery, "search_hit");
        searchHits = new HashSet<>(hits);
    }

    public List<String> getSearchHits() {
        return new ArrayList<String>(searchHits);
    }

    public SpectrumQueryWithHits addQuery(SpectrumQueryWithHits added)  {
        if(!spectraMatches(queryLine,added.queryLine)) {
            String originalstr = "original:" + queryLine;
            System.out.println(originalstr);
            String mismatchStr = "mismatch:" + added.queryLine;
            System.out.println(mismatchStr);
            System.err.println(originalstr);
            System.err.println(mismatchStr);

            throw new IllegalStateException("mismatched queries:  " + originalstr + ":vs:" + mismatchStr);

        }
        searchHits.addAll(added.searchHits);
        return this;
    }

    private boolean spectraMatches(String queryLine, String queryLine1) {
        String spectrum1 = extractSpectrum(queryLine);
        String spectrum2 = extractSpectrum(queryLine1);
        return spectrum1.equals(spectrum2);
    }

    private String extractSpectrum(String queryLine) {
        String part1 = queryLine.substring(queryLine.indexOf("spectrum=\""));
        String ret = part1.substring(0,part1.indexOf("\""));
        return ret;

    }


    public String formatBestHits(int numberHits) {
        StringBuilder sb = new StringBuilder();
        sb.append(queryLine);
        sb.append("\n");
        sb.append("<search_result>");
        sb.append(buildRankedHitsString(numberHits));
         sb.append("</search_result>");
        sb.append("\n");
        sb.append("</search_hit>");
         return sb.toString();
     }

    private String buildRankedHitsString(int numberHits) {

        StringBuilder sb = new StringBuilder();
        List<ScoredHit> scored = buildScoredHits();
        for (int i = 0; i < Math.min(numberHits,scored.size()); i++) {
            ScoredHit atRank = scored.get(i) ;
            String adjusted = XMLUtilities.substituteAttributeValue(atRank.xml,"hit_rank",Integer.toString(i + 1));
            sb.append(adjusted);
            sb.append("\n");
        }
        return sb.toString();
    }


    private List<ScoredHit> buildScoredHits() {
        List<ScoredHit> ret = new ArrayList<>();
        for (String searchHit : searchHits) {
            ret.add(new ScoredHit(searchHit));
        }
         Collections.sort(ret);
        return ret;
    }

    public class ScoredHit implements Comparable<ScoredHit>
    {
         public final String xml;
         public final double score;

        public ScoredHit(String xml) {
            this.xml = xml;
            String scoreStr = XMLUtilities.extractTag("name=\"xcorr\" value", xml);
            score = Double.parseDouble(scoreStr);
        }

        @Override
        public int compareTo(ScoredHit o) {
            if(score == o.score)
                return xml.compareTo(o.xml);
            return score < o.score ? 1 : -1;
        }
    }
}
