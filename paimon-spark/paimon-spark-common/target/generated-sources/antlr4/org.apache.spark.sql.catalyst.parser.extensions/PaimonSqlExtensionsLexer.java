// Generated from org.apache.spark.sql.catalyst.parser.extensions/PaimonSqlExtensions.g4 by ANTLR 4.9.3
package org.apache.spark.sql.catalyst.parser.extensions;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PaimonSqlExtensionsLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.9.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, CALL=7, TRUE=8, FALSE=9, 
		MAP=10, PLUS=11, MINUS=12, STRING=13, BIGINT_LITERAL=14, SMALLINT_LITERAL=15, 
		TINYINT_LITERAL=16, INTEGER_VALUE=17, EXPONENT_VALUE=18, DECIMAL_VALUE=19, 
		FLOAT_LITERAL=20, DOUBLE_LITERAL=21, BIGDECIMAL_LITERAL=22, IDENTIFIER=23, 
		BACKQUOTED_IDENTIFIER=24, SIMPLE_COMMENT=25, BRACKETED_COMMENT=26, WS=27, 
		UNRECOGNIZED=28;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "CALL", "TRUE", "FALSE", 
			"MAP", "PLUS", "MINUS", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", 
			"TINYINT_LITERAL", "INTEGER_VALUE", "EXPONENT_VALUE", "DECIMAL_VALUE", 
			"FLOAT_LITERAL", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", "IDENTIFIER", 
			"BACKQUOTED_IDENTIFIER", "DECIMAL_DIGITS", "EXPONENT", "DIGIT", "LETTER", 
			"SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "';'", "'('", "','", "')'", "'=>'", "'.'", "'CALL'", "'TRUE'", 
			"'FALSE'", "'MAP'", "'+'", "'-'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "CALL", "TRUE", "FALSE", "MAP", 
			"PLUS", "MINUS", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", 
			"INTEGER_VALUE", "EXPONENT_VALUE", "DECIMAL_VALUE", "FLOAT_LITERAL", 
			"DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", 
			"SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	  /**
	   * Verify whether current token is a valid decimal token (which contains dot).
	   * Returns true if the character that follows the token is not a digit or letter or underscore.
	   *
	   * For example:
	   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
	   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
	   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
	   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
	   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
	   * which is not a digit or letter or underscore.
	   */
	  public boolean isValidDecimal() {
	    int nextChar = _input.LA(1);
	    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
	      nextChar == '_') {
	      return false;
	    } else {
	      return true;
	    }
	  }

	  /**
	   * This method will be called when we see '/*' and try to match it as a bracketed comment.
	   * If the next character is '+', it should be parsed as hint later, and we cannot match
	   * it as a bracketed comment.
	   *
	   * Returns true if the next character is '+'.
	   */
	  public boolean isHint() {
	    int nextChar = _input.LA(1);
	    if (nextChar == '+') {
	      return true;
	    } else {
	      return false;
	    }
	  }


	public PaimonSqlExtensionsLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "PaimonSqlExtensions.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	@Override
	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 17:
			return EXPONENT_VALUE_sempred((RuleContext)_localctx, predIndex);
		case 18:
			return DECIMAL_VALUE_sempred((RuleContext)_localctx, predIndex);
		case 19:
			return FLOAT_LITERAL_sempred((RuleContext)_localctx, predIndex);
		case 20:
			return DOUBLE_LITERAL_sempred((RuleContext)_localctx, predIndex);
		case 21:
			return BIGDECIMAL_LITERAL_sempred((RuleContext)_localctx, predIndex);
		case 29:
			return BRACKETED_COMMENT_sempred((RuleContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean EXPONENT_VALUE_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return isValidDecimal();
		}
		return true;
	}
	private boolean DECIMAL_VALUE_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return isValidDecimal();
		}
		return true;
	}
	private boolean FLOAT_LITERAL_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return isValidDecimal();
		}
		return true;
	}
	private boolean DOUBLE_LITERAL_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return isValidDecimal();
		}
		return true;
	}
	private boolean BIGDECIMAL_LITERAL_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 4:
			return isValidDecimal();
		}
		return true;
	}
	private boolean BRACKETED_COMMENT_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 5:
			return !isHint();
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\36\u0143\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
		" \4!\t!\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\b\3\b\3"+
		"\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13"+
		"\3\13\3\f\3\f\3\r\3\r\3\16\3\16\3\16\3\16\7\16m\n\16\f\16\16\16p\13\16"+
		"\3\16\3\16\3\16\3\16\3\16\7\16w\n\16\f\16\16\16z\13\16\3\16\5\16}\n\16"+
		"\3\17\6\17\u0080\n\17\r\17\16\17\u0081\3\17\3\17\3\20\6\20\u0087\n\20"+
		"\r\20\16\20\u0088\3\20\3\20\3\21\6\21\u008e\n\21\r\21\16\21\u008f\3\21"+
		"\3\21\3\22\6\22\u0095\n\22\r\22\16\22\u0096\3\23\6\23\u009a\n\23\r\23"+
		"\16\23\u009b\3\23\3\23\3\23\3\23\3\23\3\23\5\23\u00a4\n\23\3\24\3\24\3"+
		"\24\3\25\6\25\u00aa\n\25\r\25\16\25\u00ab\3\25\5\25\u00af\n\25\3\25\3"+
		"\25\3\25\3\25\5\25\u00b5\n\25\3\25\3\25\3\25\5\25\u00ba\n\25\3\26\6\26"+
		"\u00bd\n\26\r\26\16\26\u00be\3\26\5\26\u00c2\n\26\3\26\3\26\3\26\3\26"+
		"\5\26\u00c8\n\26\3\26\3\26\3\26\5\26\u00cd\n\26\3\27\6\27\u00d0\n\27\r"+
		"\27\16\27\u00d1\3\27\5\27\u00d5\n\27\3\27\3\27\3\27\3\27\3\27\5\27\u00dc"+
		"\n\27\3\27\3\27\3\27\3\27\3\27\5\27\u00e3\n\27\3\30\3\30\3\30\6\30\u00e8"+
		"\n\30\r\30\16\30\u00e9\3\31\3\31\3\31\3\31\7\31\u00f0\n\31\f\31\16\31"+
		"\u00f3\13\31\3\31\3\31\3\32\6\32\u00f8\n\32\r\32\16\32\u00f9\3\32\3\32"+
		"\7\32\u00fe\n\32\f\32\16\32\u0101\13\32\3\32\3\32\6\32\u0105\n\32\r\32"+
		"\16\32\u0106\5\32\u0109\n\32\3\33\3\33\5\33\u010d\n\33\3\33\6\33\u0110"+
		"\n\33\r\33\16\33\u0111\3\34\3\34\3\35\3\35\3\36\3\36\3\36\3\36\3\36\3"+
		"\36\7\36\u011e\n\36\f\36\16\36\u0121\13\36\3\36\5\36\u0124\n\36\3\36\5"+
		"\36\u0127\n\36\3\36\3\36\3\37\3\37\3\37\3\37\3\37\3\37\7\37\u0131\n\37"+
		"\f\37\16\37\u0134\13\37\3\37\3\37\3\37\3\37\3\37\3 \6 \u013c\n \r \16"+
		" \u013d\3 \3 \3!\3!\3\u0132\2\"\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23"+
		"\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31"+
		"\61\32\63\2\65\2\67\29\2;\33=\34?\35A\36\3\2\n\4\2))^^\4\2$$^^\3\2bb\4"+
		"\2--//\3\2\62;\3\2C\\\4\2\f\f\17\17\5\2\13\f\17\17\"\"\2\u0167\2\3\3\2"+
		"\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17"+
		"\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2"+
		"\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3"+
		"\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3"+
		"\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\3C\3\2\2\2\5E\3\2\2"+
		"\2\7G\3\2\2\2\tI\3\2\2\2\13K\3\2\2\2\rN\3\2\2\2\17P\3\2\2\2\21U\3\2\2"+
		"\2\23Z\3\2\2\2\25`\3\2\2\2\27d\3\2\2\2\31f\3\2\2\2\33|\3\2\2\2\35\177"+
		"\3\2\2\2\37\u0086\3\2\2\2!\u008d\3\2\2\2#\u0094\3\2\2\2%\u00a3\3\2\2\2"+
		"\'\u00a5\3\2\2\2)\u00b9\3\2\2\2+\u00cc\3\2\2\2-\u00e2\3\2\2\2/\u00e7\3"+
		"\2\2\2\61\u00eb\3\2\2\2\63\u0108\3\2\2\2\65\u010a\3\2\2\2\67\u0113\3\2"+
		"\2\29\u0115\3\2\2\2;\u0117\3\2\2\2=\u012a\3\2\2\2?\u013b\3\2\2\2A\u0141"+
		"\3\2\2\2CD\7=\2\2D\4\3\2\2\2EF\7*\2\2F\6\3\2\2\2GH\7.\2\2H\b\3\2\2\2I"+
		"J\7+\2\2J\n\3\2\2\2KL\7?\2\2LM\7@\2\2M\f\3\2\2\2NO\7\60\2\2O\16\3\2\2"+
		"\2PQ\7E\2\2QR\7C\2\2RS\7N\2\2ST\7N\2\2T\20\3\2\2\2UV\7V\2\2VW\7T\2\2W"+
		"X\7W\2\2XY\7G\2\2Y\22\3\2\2\2Z[\7H\2\2[\\\7C\2\2\\]\7N\2\2]^\7U\2\2^_"+
		"\7G\2\2_\24\3\2\2\2`a\7O\2\2ab\7C\2\2bc\7R\2\2c\26\3\2\2\2de\7-\2\2e\30"+
		"\3\2\2\2fg\7/\2\2g\32\3\2\2\2hn\7)\2\2im\n\2\2\2jk\7^\2\2km\13\2\2\2l"+
		"i\3\2\2\2lj\3\2\2\2mp\3\2\2\2nl\3\2\2\2no\3\2\2\2oq\3\2\2\2pn\3\2\2\2"+
		"q}\7)\2\2rx\7$\2\2sw\n\3\2\2tu\7^\2\2uw\13\2\2\2vs\3\2\2\2vt\3\2\2\2w"+
		"z\3\2\2\2xv\3\2\2\2xy\3\2\2\2y{\3\2\2\2zx\3\2\2\2{}\7$\2\2|h\3\2\2\2|"+
		"r\3\2\2\2}\34\3\2\2\2~\u0080\5\67\34\2\177~\3\2\2\2\u0080\u0081\3\2\2"+
		"\2\u0081\177\3\2\2\2\u0081\u0082\3\2\2\2\u0082\u0083\3\2\2\2\u0083\u0084"+
		"\7N\2\2\u0084\36\3\2\2\2\u0085\u0087\5\67\34\2\u0086\u0085\3\2\2\2\u0087"+
		"\u0088\3\2\2\2\u0088\u0086\3\2\2\2\u0088\u0089\3\2\2\2\u0089\u008a\3\2"+
		"\2\2\u008a\u008b\7U\2\2\u008b \3\2\2\2\u008c\u008e\5\67\34\2\u008d\u008c"+
		"\3\2\2\2\u008e\u008f\3\2\2\2\u008f\u008d\3\2\2\2\u008f\u0090\3\2\2\2\u0090"+
		"\u0091\3\2\2\2\u0091\u0092\7[\2\2\u0092\"\3\2\2\2\u0093\u0095\5\67\34"+
		"\2\u0094\u0093\3\2\2\2\u0095\u0096\3\2\2\2\u0096\u0094\3\2\2\2\u0096\u0097"+
		"\3\2\2\2\u0097$\3\2\2\2\u0098\u009a\5\67\34\2\u0099\u0098\3\2\2\2\u009a"+
		"\u009b\3\2\2\2\u009b\u0099\3\2\2\2\u009b\u009c\3\2\2\2\u009c\u009d\3\2"+
		"\2\2\u009d\u009e\5\65\33\2\u009e\u00a4\3\2\2\2\u009f\u00a0\5\63\32\2\u00a0"+
		"\u00a1\5\65\33\2\u00a1\u00a2\6\23\2\2\u00a2\u00a4\3\2\2\2\u00a3\u0099"+
		"\3\2\2\2\u00a3\u009f\3\2\2\2\u00a4&\3\2\2\2\u00a5\u00a6\5\63\32\2\u00a6"+
		"\u00a7\6\24\3\2\u00a7(\3\2\2\2\u00a8\u00aa\5\67\34\2\u00a9\u00a8\3\2\2"+
		"\2\u00aa\u00ab\3\2\2\2\u00ab\u00a9\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac\u00ae"+
		"\3\2\2\2\u00ad\u00af\5\65\33\2\u00ae\u00ad\3\2\2\2\u00ae\u00af\3\2\2\2"+
		"\u00af\u00b0\3\2\2\2\u00b0\u00b1\7H\2\2\u00b1\u00ba\3\2\2\2\u00b2\u00b4"+
		"\5\63\32\2\u00b3\u00b5\5\65\33\2\u00b4\u00b3\3\2\2\2\u00b4\u00b5\3\2\2"+
		"\2\u00b5\u00b6\3\2\2\2\u00b6\u00b7\7H\2\2\u00b7\u00b8\6\25\4\2\u00b8\u00ba"+
		"\3\2\2\2\u00b9\u00a9\3\2\2\2\u00b9\u00b2\3\2\2\2\u00ba*\3\2\2\2\u00bb"+
		"\u00bd\5\67\34\2\u00bc\u00bb\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00bc\3"+
		"\2\2\2\u00be\u00bf\3\2\2\2\u00bf\u00c1\3\2\2\2\u00c0\u00c2\5\65\33\2\u00c1"+
		"\u00c0\3\2\2\2\u00c1\u00c2\3\2\2\2\u00c2\u00c3\3\2\2\2\u00c3\u00c4\7F"+
		"\2\2\u00c4\u00cd\3\2\2\2\u00c5\u00c7\5\63\32\2\u00c6\u00c8\5\65\33\2\u00c7"+
		"\u00c6\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00c9\3\2\2\2\u00c9\u00ca\7F"+
		"\2\2\u00ca\u00cb\6\26\5\2\u00cb\u00cd\3\2\2\2\u00cc\u00bc\3\2\2\2\u00cc"+
		"\u00c5\3\2\2\2\u00cd,\3\2\2\2\u00ce\u00d0\5\67\34\2\u00cf\u00ce\3\2\2"+
		"\2\u00d0\u00d1\3\2\2\2\u00d1\u00cf\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d4"+
		"\3\2\2\2\u00d3\u00d5\5\65\33\2\u00d4\u00d3\3\2\2\2\u00d4\u00d5\3\2\2\2"+
		"\u00d5\u00d6\3\2\2\2\u00d6\u00d7\7D\2\2\u00d7\u00d8\7F\2\2\u00d8\u00e3"+
		"\3\2\2\2\u00d9\u00db\5\63\32\2\u00da\u00dc\5\65\33\2\u00db\u00da\3\2\2"+
		"\2\u00db\u00dc\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd\u00de\7D\2\2\u00de\u00df"+
		"\7F\2\2\u00df\u00e0\3\2\2\2\u00e0\u00e1\6\27\6\2\u00e1\u00e3\3\2\2\2\u00e2"+
		"\u00cf\3\2\2\2\u00e2\u00d9\3\2\2\2\u00e3.\3\2\2\2\u00e4\u00e8\59\35\2"+
		"\u00e5\u00e8\5\67\34\2\u00e6\u00e8\7a\2\2\u00e7\u00e4\3\2\2\2\u00e7\u00e5"+
		"\3\2\2\2\u00e7\u00e6\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9\u00e7\3\2\2\2\u00e9"+
		"\u00ea\3\2\2\2\u00ea\60\3\2\2\2\u00eb\u00f1\7b\2\2\u00ec\u00f0\n\4\2\2"+
		"\u00ed\u00ee\7b\2\2\u00ee\u00f0\7b\2\2\u00ef\u00ec\3\2\2\2\u00ef\u00ed"+
		"\3\2\2\2\u00f0\u00f3\3\2\2\2\u00f1\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2"+
		"\u00f4\3\2\2\2\u00f3\u00f1\3\2\2\2\u00f4\u00f5\7b\2\2\u00f5\62\3\2\2\2"+
		"\u00f6\u00f8\5\67\34\2\u00f7\u00f6\3\2\2\2\u00f8\u00f9\3\2\2\2\u00f9\u00f7"+
		"\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00ff\7\60\2\2"+
		"\u00fc\u00fe\5\67\34\2\u00fd\u00fc\3\2\2\2\u00fe\u0101\3\2\2\2\u00ff\u00fd"+
		"\3\2\2\2\u00ff\u0100\3\2\2\2\u0100\u0109\3\2\2\2\u0101\u00ff\3\2\2\2\u0102"+
		"\u0104\7\60\2\2\u0103\u0105\5\67\34\2\u0104\u0103\3\2\2\2\u0105\u0106"+
		"\3\2\2\2\u0106\u0104\3\2\2\2\u0106\u0107\3\2\2\2\u0107\u0109\3\2\2\2\u0108"+
		"\u00f7\3\2\2\2\u0108\u0102\3\2\2\2\u0109\64\3\2\2\2\u010a\u010c\7G\2\2"+
		"\u010b\u010d\t\5\2\2\u010c\u010b\3\2\2\2\u010c\u010d\3\2\2\2\u010d\u010f"+
		"\3\2\2\2\u010e\u0110\5\67\34\2\u010f\u010e\3\2\2\2\u0110\u0111\3\2\2\2"+
		"\u0111\u010f\3\2\2\2\u0111\u0112\3\2\2\2\u0112\66\3\2\2\2\u0113\u0114"+
		"\t\6\2\2\u01148\3\2\2\2\u0115\u0116\t\7\2\2\u0116:\3\2\2\2\u0117\u0118"+
		"\7/\2\2\u0118\u0119\7/\2\2\u0119\u011f\3\2\2\2\u011a\u011b\7^\2\2\u011b"+
		"\u011e\7\f\2\2\u011c\u011e\n\b\2\2\u011d\u011a\3\2\2\2\u011d\u011c\3\2"+
		"\2\2\u011e\u0121\3\2\2\2\u011f\u011d\3\2\2\2\u011f\u0120\3\2\2\2\u0120"+
		"\u0123\3\2\2\2\u0121\u011f\3\2\2\2\u0122\u0124\7\17\2\2\u0123\u0122\3"+
		"\2\2\2\u0123\u0124\3\2\2\2\u0124\u0126\3\2\2\2\u0125\u0127\7\f\2\2\u0126"+
		"\u0125\3\2\2\2\u0126\u0127\3\2\2\2\u0127\u0128\3\2\2\2\u0128\u0129\b\36"+
		"\2\2\u0129<\3\2\2\2\u012a\u012b\7\61\2\2\u012b\u012c\7,\2\2\u012c\u012d"+
		"\3\2\2\2\u012d\u0132\6\37\7\2\u012e\u0131\5=\37\2\u012f\u0131\13\2\2\2"+
		"\u0130\u012e\3\2\2\2\u0130\u012f\3\2\2\2\u0131\u0134\3\2\2\2\u0132\u0133"+
		"\3\2\2\2\u0132\u0130\3\2\2\2\u0133\u0135\3\2\2\2\u0134\u0132\3\2\2\2\u0135"+
		"\u0136\7,\2\2\u0136\u0137\7\61\2\2\u0137\u0138\3\2\2\2\u0138\u0139\b\37"+
		"\2\2\u0139>\3\2\2\2\u013a\u013c\t\t\2\2\u013b\u013a\3\2\2\2\u013c\u013d"+
		"\3\2\2\2\u013d\u013b\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u013f\3\2\2\2\u013f"+
		"\u0140\b \2\2\u0140@\3\2\2\2\u0141\u0142\13\2\2\2\u0142B\3\2\2\2+\2ln"+
		"vx|\u0081\u0088\u008f\u0096\u009b\u00a3\u00ab\u00ae\u00b4\u00b9\u00be"+
		"\u00c1\u00c7\u00cc\u00d1\u00d4\u00db\u00e2\u00e7\u00e9\u00ef\u00f1\u00f9"+
		"\u00ff\u0106\u0108\u010c\u0111\u011d\u011f\u0123\u0126\u0130\u0132\u013d"+
		"\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}