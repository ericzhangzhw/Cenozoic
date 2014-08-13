package com.graphconcern.cenozoic.search.analysis;

import java.io.Reader;

import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Version;

public class SpecificKeywordAnalyzer extends Analyzer {

    private final Version version;

    public SpecificKeywordAnalyzer(final Version version) {
        super();
        this.version = version;
    }


	@Override
	protected TokenStreamComponents createComponents(final String fieldName,
			final Reader reader) {	
        final Tokenizer source = new KeywordTokenizer(reader);
        return new TokenStreamComponents(source, new LowerCaseFilter(this.version, source));
	}

}
